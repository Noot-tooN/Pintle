//! Bounded static credential policy, separate from coordination/domain state.
//! The adapter supplies secret JSON, mode, bearer token, and authority time.
use serde::Deserialize;
use subtle::ConstantTimeEq;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Unauthorized,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub tenant: String,
    pub subject: String,
    pub namespaces: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    token: String,
    tenant: String,
    subject: String,
    namespaces: Vec<String>,
    #[serde(default)]
    not_before_ms: Option<String>,
    #[serde(default)]
    expires_at_ms: Option<String>,
}
pub fn identifier(s: &str, max: usize) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}
fn time(v: &Option<String>, default: u64) -> Result<u64, Error> {
    match v {
        None => Ok(default),
        Some(s) => {
            let n = s.parse::<u64>().map_err(|_| Error::Configuration)?;
            if n > (1 << 53) - 1 || n.to_string() != *s {
                return Err(Error::Configuration);
            };
            Ok(n)
        }
    }
}
pub fn authenticate(
    json: &str,
    token: &str,
    now: u64,
    static_mode: bool,
) -> Result<Identity, Error> {
    if json.len() > 16384 || now > (1 << 53) - 1 {
        return Err(Error::Configuration);
    }
    let keys: Vec<Credential> = serde_json::from_str(json).map_err(|_| Error::Configuration)?;
    if keys.is_empty() || keys.len() > 16 {
        return Err(Error::Configuration);
    }
    let mut found = None;
    for (i, k) in keys.iter().enumerate() {
        let start = time(&k.not_before_ms, 0)?;
        let end = time(&k.expires_at_ms, (1 << 53) - 1)?;
        if !(32..=256).contains(&k.token.len())
            || !k.token.bytes().all(|b| b.is_ascii_alphanumeric())
            || !identifier(&k.tenant, 64)
            || !identifier(&k.subject, 32)
            || k.namespaces.is_empty()
            || k.namespaces.len() > 16
            || k.namespaces.iter().any(|s| !identifier(s, 64))
            || keys[..i].iter().any(|other| other.token == k.token)
            || start >= end
            || (static_mode && k.expires_at_ms.is_none())
        {
            return Err(Error::Configuration);
        }
        let matches = bool::from(k.token.as_bytes().ct_eq(token.as_bytes()));
        if matches && now >= start && now < end {
            found = Some(Identity {
                tenant: k.tenant.clone(),
                subject: k.subject.clone(),
                namespaces: k.namespaces.clone(),
            });
        }
    }
    found.ok_or(Error::Unauthorized)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn key(token: &str) -> serde_json::Value {
        serde_json::json!({"token":token.repeat(32),"tenant":"tenant","subject":"alice","namespaces":["test"],"not_before_ms":"1000","expires_at_ms":"2000"})
    }
    #[test]
    fn exact_time_boundaries_and_rotation() {
        let a = key("a");
        let b = key("b");
        let json = serde_json::json!([a.clone(), b.clone()]).to_string();
        for now in [999, 1000, 1999, 2000, 2001] {
            assert_eq!(
                authenticate(&json, &"a".repeat(32), now, true).is_ok(),
                (1000..2000).contains(&now)
            );
        }
        assert_eq!(
            authenticate(&json, &"a".repeat(32), 1000, true),
            authenticate(&json, &"b".repeat(32), 1000, true)
        );
        assert_eq!(
            authenticate(
                &serde_json::json!([b]).to_string(),
                &"a".repeat(32),
                1000,
                true
            ),
            Err(Error::Unauthorized)
        );
    }
    #[test]
    fn malformed_configuration_always_fails_closed() {
        let valid = key("a");
        for (field, value) in [
            ("token", serde_json::json!("short")),
            ("tenant", serde_json::json!("bad/tenant")),
            ("namespaces", serde_json::json!(["*"])),
            ("expires_at_ms", serde_json::json!("02000")),
            ("expires_at_ms", serde_json::json!("9007199254740992")),
            ("not_before_ms", serde_json::json!("2000")),
            ("extra", serde_json::json!(true)),
        ] {
            let mut bad = valid.clone();
            bad["token"] = serde_json::json!("b".repeat(32));
            bad[field] = value;
            assert_eq!(
                authenticate(
                    &serde_json::json!([valid.clone(), bad]).to_string(),
                    &"a".repeat(32),
                    1000,
                    true
                ),
                Err(Error::Configuration)
            );
        }
        assert_eq!(
            authenticate(
                &serde_json::json!([valid.clone(), valid.clone()]).to_string(),
                &"a".repeat(32),
                1000,
                true
            ),
            Err(Error::Configuration)
        );
        let mut no_expiry = valid.clone();
        no_expiry.as_object_mut().unwrap().remove("expires_at_ms");
        assert!(
            authenticate(
                &serde_json::json!([no_expiry.clone()]).to_string(),
                &"a".repeat(32),
                1000,
                false
            )
            .is_ok()
        );
        assert_eq!(
            authenticate(
                &serde_json::json!([no_expiry]).to_string(),
                &"a".repeat(32),
                1000,
                true
            ),
            Err(Error::Configuration)
        );
        for json in ["[]".into(), "{}".into(), " ".repeat(16385)] {
            assert_eq!(
                authenticate(&json, "", 1000, true),
                Err(Error::Configuration)
            );
        }
    }
}
