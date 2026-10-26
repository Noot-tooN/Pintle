//! Test-only checkpoints around (never inside) the synchronous transaction.
//! The default build contains no controller HTTP client or caller hook headers.
use worker::*;

#[derive(Default, Clone)]
pub(crate) struct Schedule {
    #[cfg(feature = "test-hooks")]
    endpoint: Option<String>,
    #[cfg(feature = "test-hooks")]
    call: String,
    time: Option<u64>,
    signal_timer: bool,
}
impl Schedule {
    pub fn new(_env: &Env, _req: &Request) -> Result<Self> {
        #[cfg(feature = "test-hooks")]
        if let Ok(endpoint) = _env.var("PINTLE_TEST_SCHEDULER") {
            let endpoint = endpoint.to_string();
            if !endpoint.starts_with("http://127.0.0.1:") {
                return Err(Error::RustError("test controller must be loopback".into()));
            }
            if let Some(call) = _req.headers().get("x-pintle-test-call")? {
                if call.is_empty()
                    || call.len() > 128
                    || !call.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                {
                    return Err(Error::RustError("invalid test call".into()));
                }
                return Ok(Self {
                    endpoint: Some(endpoint),
                    call,
                    time: None,
                    signal_timer: false,
                });
            }
        }
        Ok(Self::default())
    }
    pub fn now(&self) -> u64 {
        self.time.unwrap_or_else(|| Date::now().as_millis())
    }
    pub fn virtual_time(&self) -> bool {
        self.time.is_some()
    }
    pub async fn timeout(mut self, duration: u64) -> Result<()> {
        if self.virtual_time() {
            if self.signal_timer {
                self.at("deadline_timer").await?;
            } else {
                // Logical deadline comparisons are driven at explicit checkpoints.
                futures_util::future::pending::<()>().await;
            }
        } else {
            Delay::from(std::time::Duration::from_millis(duration)).await;
        }
        Ok(())
    }
    pub async fn sync(&mut self, storage: &Storage) -> Result<()> {
        storage.sync().await?;
        // A controlled failure here exercises the same caller error path as a
        // failed durability acknowledgement, without pretending to break SQLite.
        self.at("after_sync").await?;
        Ok(())
    }
    pub fn forward(&self, _headers: &Headers) -> Result<()> {
        #[cfg(feature = "test-hooks")]
        if self.endpoint.is_some() {
            _headers.set("x-pintle-test-call", &self.call)?;
        }
        Ok(())
    }
    pub async fn at(&mut self, _stage: &str) -> Result<Option<crate::Fault>> {
        #[cfg(feature = "test-hooks")]
        if let Some(endpoint) = &self.endpoint {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Directive {
                now: u64,
                #[serde(default)]
                fail: bool,
                fault: Option<crate::Fault>,
                #[serde(default)]
                timer: bool,
            }
            let url = Url::parse(&format!("{endpoint}/{}/{_stage}", self.call))?;
            let mut response = Fetch::Url(url).send().await?;
            if response.status_code() != 200 {
                return Err(Error::RustError(
                    "test controller rejected checkpoint".into(),
                ));
            }
            let directive: Directive = response.json().await?;
            self.time = Some(directive.now);
            self.signal_timer |= directive.timer;
            if directive.fail {
                return Err(Error::RustError(
                    "injected acknowledgement/transport failure".into(),
                ));
            }
            return Ok(directive.fault);
        }
        Ok(None)
    }
}
