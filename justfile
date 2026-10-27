# Pintle's development commands. Use ./bin/just, or activate Hermit and use just.
# Recipes run as single Bash scripts; arguments are passed without interpolation.
set default-script
set script-interpreter := ['bash', '-euo', 'pipefail']
set positional-arguments

export SHELL := '/bin/bash'
export PATH := justfile_directory() / 'bin' + ':' + env('PATH')

# List the supported development commands.
[default]
help:
    ./bin/just --list

# Install pinned Hermit tools and the Rust toolchain (also used by core-only CI).
bootstrap-core:
    ./bin/hermit install
    ./bin/rustup toolchain install 1.99.1 --no-self-update --profile minimal --component rustfmt --component clippy --target wasm32-unknown-unknown

# Install all pinned dependencies; requires download access on a fresh checkout.
bootstrap: bootstrap-core
    source ./bin/activate-hermit
    npm ci --ignore-scripts
    # worker-build has no upstream Hermit package; install into project CARGO_HOME.
    cargo install worker-build --version 0.8.7 --locked
    go mod download

# Build release Wasm; optional worker-build arguments follow the recipe name.
build *args:
    source ./bin/activate-hermit
    cd crates/cloudflare
    worker-build --release --locked "$@"

# Generate Go/Connect and Rust Protobuf types into the checkout or an output directory.
generate out_dir='.':
    mkdir -p "$1"
    out_dir="$(cd "$1" && pwd)"
    descriptor_file="$(mktemp)"
    trap 'rm -f "$descriptor_file"' EXIT
    ./bin/buf lint
    ./bin/buf format --diff --exit-code
    ./bin/buf generate --output "$out_dir"
    ./bin/buf build --as-file-descriptor-set -o "$descriptor_file"
    ./bin/cargo run --locked --quiet -p pintle-proto-gen -- "$descriptor_file" "$out_dir/crates/protocol/src/generated"
    ./bin/rustfmt --edition 2024 "$out_dir/crates/protocol/src/generated/pintle.v1.rs"

# Check schema compatibility, formatting and generated-code drift without rewriting sources.
check-proto:
    scratch_dir="$(mktemp -d)"
    trap 'rm -rf "$scratch_dir"' EXIT
    ./bin/just generate "$scratch_dir"
    diff -ru gen/go "$scratch_dir/gen/go"
    diff -ru crates/protocol/src/generated "$scratch_dir/crates/protocol/src/generated"
    ./bin/go run ./tools/proto-compat proto-baseline/p0b.binpb "$scratch_dir/baseline.binpb"
    ./bin/buf breaking --against "$scratch_dir/baseline.binpb"
    if [[ -n "${PINTLE_PROTO_BASE_REF:-}" ]] && git cat-file -e "${PINTLE_PROTO_BASE_REF}:buf.yaml" 2>/dev/null; then
      ./bin/buf build ".git#ref=${PINTLE_PROTO_BASE_REF}" --as-file-descriptor-set -o "$scratch_dir/base-ref.binpb"
      ./bin/go run ./tools/proto-compat "$scratch_dir/base-ref.binpb" "$scratch_dir/base-ref-normalized.binpb"
      ./bin/buf breaking --against "$scratch_dir/base-ref-normalized.binpb"
    fi

# Check Rust formatting and portable core/auth lints (no Wasm runtime required).
check-core:
    ./bin/cargo fmt --all -- --check
    ./bin/cargo clippy --locked -p pintle-core -p pintle-auth --all-targets -- -D warnings

# Check all formatting, schema/codegen, Rust lints and Go vet, including test hooks.
check: check-proto check-core staging-check
    ./bin/just --fmt --check
    ./bin/cargo clippy --locked -p pintle-protocol -p pintle-proto-gen --all-targets -- -D warnings
    ./bin/cargo clippy --locked -p pintle-cloudflare --target wasm32-unknown-unknown -- -D warnings
    ./bin/cargo clippy --locked -p pintle-cloudflare --target wasm32-unknown-unknown --features test-hooks -- -D warnings
    ./bin/go vet ./sdk/go/... ./examples/... ./tools/proto-compat ./tests/integration ./tests/hosted
    ./bin/go vet -tags=testhooks ./tests/integration

# Run native Rust core/auth tests; optionally select a test-name substring.
test-core filter='':
    ./bin/cargo test --locked -p pintle-core -p pintle-auth "$1" -- --nocapture

# Run Go SDK and hosted-runner guard tests with the race detector; optional regexp.
test-sdk filter='.*':
    ./bin/go test -race -count=1 -timeout=3m -v ./gen/go/... ./sdk/go/... ./examples/... ./tools/proto-compat ./tests/hosted -run "$1"

# Build normal Wasm and test real local Wrangler/SQLite; optional Go regexp.
test-runtime filter='.*':
    ./bin/just build
    ./bin/go test -race -count=1 -timeout=3m -v ./tests/integration -run "$1"

# Run controlled-clock/fault tests; always restore normal Wasm, including on failure.
test-hardening filter='TestMutexStorage|TestMutexSchedules|TestP1Deterministic|TestP11Deterministic|TestP2|TestP3CredentialTimeBoundaries':
    restore() {
      status=$?
      trap - EXIT
      ./bin/just build || status=$?
      exit "$status"
    }
    trap restore EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    ./bin/cargo clippy --locked -p pintle-cloudflare --target wasm32-unknown-unknown --features test-hooks -- -D warnings
    ./bin/just build --features test-hooks
    ./bin/go test -race -tags=testhooks -count=1 -timeout=3m -v ./tests/integration -run "$1"

# Run the complete local verification suite, sequentially against shared Wasm artifacts.
test:
    ./bin/just check
    ./bin/just check-sdk-package
    ./bin/just test-core
    ./bin/just test-sdk
    ./bin/just test-runtime
    ./bin/just test-hardening

# Build the actual example as a consumer outside the repository's Go module.
check-sdk-package:
    scratch_dir="$(mktemp -d)"
    trap 'rm -rf "$scratch_dir"' EXIT
    project_dir="$PWD"
    cp examples/mutex/main.go "$scratch_dir/main.go"
    cp go.sum "$scratch_dir/go.sum"
    cd "$scratch_dir"
    export GOWORK=off GOPROXY=off GOSUMDB=off
    "$project_dir/bin/go" mod init example.com/pintle-consumer
    "$project_dir/bin/go" mod edit -go=1.27.3 -require=github.com/Noot-tooN/Pintle@v0.0.0 "-replace=github.com/Noot-tooN/Pintle=$project_dir"
    "$project_dir/bin/go" mod tidy
    "$project_dir/bin/go" build -mod=readonly -o consumer .
    ./consumer -h

# Run all example commands against an automatically managed local Wrangler.
example-local:
    ./bin/just test-runtime '^TestSDKExamples$'

# Run one example against an explicitly configured endpoint; no server deployment.
example mode='lifecycle':
    ./bin/go run ./examples/mutex -mode "$1"

# Query online npm and Rust/Go advisory databases; separate from correctness tests.
audit:
    mkdir -p test-results
    ./bin/npm audit --json > test-results/npm-audit.json
    ./bin/node tools/security-audit.mjs test-results/security-audit.json

# Optional local Go scan. Supply a local rules file; scanner defaults to PATH.
# State stays in Pintle. Default scanner ignores exclude test/generated files.
security-scan rules scanner='opengrep':
    test -f "$1"
    scan_state="$PWD/.hermit/opengrep"
    umask 077
    mkdir -p "$scan_state/config" "$scan_state/cache" "$scan_state/tmp" test-results/opengrep
    export XDG_CONFIG_HOME="$scan_state/config"
    export XDG_CACHE_HOME="$scan_state/cache"
    export TMPDIR="$scan_state/tmp"
    export SEMGREP_LOG_FILE="$scan_state/semgrep.log"
    export SEMGREP_SETTINGS_FILE="$scan_state/settings.yml"
    export OPENGREP_ENABLE_VERSION_CHECK=0
    "$2" --version > test-results/opengrep/version.txt
    "$2" scan --config "$1" --disable-version-check --no-git-ignore --jobs 4 --timeout 5 --strict --error --json sdk examples tools/proto-compat > test-results/opengrep/current.json 2> test-results/opengrep/current.stderr

# Validate the isolated staging target locally; no Cloudflare calls or credentials.
staging-check:
    ./bin/node --input-type=module <<'JS'
    import {readFileSync} from 'node:fs';
    import assert from 'node:assert/strict';
    const c = JSON.parse(readFileSync('wrangler.staging.jsonc', 'utf8'));
    assert.equal(c.name, 'pintle-staging');
    assert.equal(c.account_id, undefined, 'select account explicitly via CLOUDFLARE_ACCOUNT_ID');
    assert.equal(c.main, 'crates/cloudflare/build/worker/shim.mjs');
    assert.equal(c.workers_dev, true);
    assert.equal(c.preview_urls, false);
    assert.deepEqual(c.observability, {enabled:true});
    assert.equal(c.routes, undefined);
    assert.equal(c.route, undefined);
    assert.deepEqual(c.vars, {P2_ENABLED:'true', PINTLE_AUTH_MODE:'static', PINTLE_METRICS:'true'});
    assert.deepEqual(c.build, {command:'./bin/just build'});
    assert.deepEqual(c.durable_objects, {bindings:[{name:'AUTHORITIES',class_name:'PintleAuthority'}]});
    assert.deepEqual(c.migrations, [{tag:'p0-sqlite-v1',new_sqlite_classes:['PintleAuthority']}]);
    console.log('Staging configuration: isolated pintle-staging Worker, SQLite authority, static auth, normal build.');
    JS

# Read-only account membership check; select a non-secret account ID explicitly.
staging-account:
    mkdir -p test-results
    ./bin/node --input-type=module <<'JS'
    import {execFileSync} from 'node:child_process';
    const id = process.env.CLOUDFLARE_ACCOUNT_ID;
    if (!/^[a-f0-9]{32}$/.test(id || '')) throw Error('Set CLOUDFLARE_ACCOUNT_ID to the intended account ID');
    const raw = execFileSync('./bin/node', ['node_modules/wrangler/bin/wrangler.js','whoami','--json'], {
      encoding:'utf8', env:{...process.env,WRANGLER_HIDE_BANNER:'true',WRANGLER_LOG_PATH:'test-results/staging-account.log'}
    });
    const identity = JSON.parse(raw);
    const account = identity.accounts?.find(a => a.id === id);
    if (!identity.loggedIn || !account) throw Error('Selected account is not in the authenticated account list');
    console.log(`Verified account: ${account.name} (${account.id})`);
    JS

# Build and package staging locally. --dry-run never uploads or creates resources.
staging-dry-run: staging-check
    mkdir -p test-results/staging-bundle
    WRANGLER_HIDE_BANNER=true WRANGLER_LOG_PATH=test-results/staging-dry-run.log ./bin/node node_modules/wrangler/bin/wrangler.js deploy --dry-run --config wrangler.staging.jsonc --outdir test-results/staging-bundle

# Remote mutation: deploy staging ONLY when authorized.
staging-deploy: staging-check staging-account
    ./bin/node node_modules/wrangler/bin/wrangler.js deploy --config wrangler.staging.jsonc

# Remote mutation: supply credential JSON via stdin or Wrangler's hidden prompt.
staging-secret: staging-check staging-account
    ./bin/node node_modules/wrangler/bin/wrangler.js secret put PINTLE_CREDENTIALS --config wrangler.staging.jsonc

# Explicit bounded remote test run; requires staging URL and token in environment.
test-staging:
    : "${PINTLE_STAGING_URL:?Set the deployed pintle-staging workers.dev URL}"
    : "${PINTLE_STAGING_TOKEN:?Set the staging application token}"
    PINTLE_RUN_HOSTED=1 ./bin/go test -race -count=1 -timeout=3m -v ./tests/hosted -run '^TestStaging$'

# Live staging logs; caller must discard raw metadata and retain only allowlisted events.
staging-tail:
    WRANGLER_WRITE_LOGS=false ./bin/node node_modules/wrangler/bin/wrangler.js tail --config wrangler.staging.jsonc --format pretty

# Explicit remote rehearsal: normal staging build/deploy, one redeploy, two key updates.
# Requires a private .hermit/staging/credentials.json; never runs in CI/default tests.
test-staging-lifecycle: staging-check
    : "${PINTLE_STAGING_URL:?Set the deployed pintle-staging workers.dev URL}"
    PINTLE_RUN_HOSTED_LIFECYCLE=1 ./bin/go test -race -count=1 -timeout=12m -v ./tests/hosted -run '^TestStagingLifecycle$'

# Existing staging deployment only: observe real alarms using scoped persisted logs.
# Reads private credentials and observability-token; no deploy or secret updates.
test-staging-alarms: staging-check staging-origin
    PINTLE_RUN_HOSTED_ALARMS=1 ./bin/go test -race -count=1 -timeout=10m -v ./tests/hosted -run '^TestStagingAlarms$'

# Bounded existing-staging rehearsal: <=600 RPCs, six resources, 16 concurrent calls.
# Verify Free-plan headroom before invoking; no deployment or credential changes.
test-staging-capacity: staging-check staging-origin
    PINTLE_RUN_HOSTED_CAPACITY=1 ./bin/go test -race -count=1 -timeout=7m -v ./tests/hosted -run '^TestStagingCapacity$'

# Read-only: require the test origin to belong to the selected account before mutation.
staging-origin: staging-account
    ./bin/node --input-type=module <<'JS'
    import {execFileSync} from 'node:child_process';
    const auth=JSON.parse(execFileSync('./bin/node',['node_modules/wrangler/bin/wrangler.js','auth','token','--json'],{encoding:'utf8',env:{...process.env,WRANGLER_HIDE_BANNER:'true',WRANGLER_WRITE_LOGS:'false'}}));
    const response=await fetch(`https://api.cloudflare.com/client/v4/accounts/${process.env.CLOUDFLARE_ACCOUNT_ID}/workers/subdomain`,{headers:{Authorization:`Bearer ${auth.token}`},signal:AbortSignal.timeout(15000)});
    const data=await response.json();
    if(!response.ok || !data.success || !data.result?.subdomain) throw Error('Cannot verify account subdomain');
    const expected=`https://pintle-staging.${data.result.subdomain}.workers.dev`;
    if(process.env.PINTLE_STAGING_URL!==expected) throw Error('Staging origin does not match selected account');
    console.log(`Verified staging origin: ${expected}`);
    JS

# Read-only persisted invocation metrics for the last bounded staging-capacity run.
# Raw provider events/headers stay in memory; only numeric aggregates are written.
staging-capacity-metrics: staging-check
    ./bin/node --input-type=module <<'JS'
    import {readFileSync,writeFileSync,statSync} from 'node:fs';
    const run=JSON.parse(readFileSync('test-results/staging-capacity.json','utf8'));
    const account=process.env.CLOUDFLARE_ACCOUNT_ID;
    if(!/^[a-f0-9]{32}$/.test(account||'')) throw Error('Explicit account required');
    const path='.hermit/staging/observability-token';
    if(statSync(path).mode&0o077) throw Error('Observability token must be private');
    const token=readFileSync(path,'utf8').trim();
    const from=Date.parse(run.started_at),to=Date.parse(run.finished_at)+1000;
    if(!Number.isFinite(from)||!Number.isFinite(to)||to<=from||to-from>361000) throw Error('Invalid bounded run window');
    const filter=(key,value)=>({key,operation:'eq',type:'string',value});
    const body={queryId:'pintle-capacity-metrics',dry:true,view:'events',limit:2000,timeframe:{from,to},parameters:{filters:[filter('$metadata.service','pintle-staging'),filter('$metadata.type','cf-worker-event')]}};
    const collected=new Map();let events=[],queries=0;
    for(;queries<12;){
      queries++;
      const res=await fetch(`https://api.cloudflare.com/client/v4/accounts/${account}/workers/observability/telemetry/query`,{method:'POST',redirect:'error',headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(15000)});
      if(!res.ok) throw Error(`Telemetry query HTTP ${res.status}`);
      const reader=res.body.getReader();const chunks=[];let size=0;
      for(;;){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>8*1024*1024){await reader.cancel();throw Error('Telemetry response exceeds 8 MiB');}chunks.push(value);}
      let data;try{data=JSON.parse(Buffer.concat(chunks).toString());}catch{throw Error('Invalid telemetry response');}
      if(!data.success||!data.result?.events) throw Error('Telemetry API rejected query');
      if(data.result.events.count>=2000) throw Error('Telemetry window truncated');
      for(const e of data.result.events.events??[]){
        if(e.$metadata?.service!=='pintle-staging'||e.$metadata?.type!=='cf-worker-event'||!e.$metadata.id)throw Error('Unexpected telemetry source');
        collected.set(e.$metadata.id,e);
      }
      if(collected.size>=2000)throw Error('Telemetry union exceeds event budget');
      events=[...collected.values()];
      const gateways=events.filter(e=>e.$workers?.entrypoint!=='PintleAuthority').length;
      if(gateways>=run.rpc_sent)break;
      if(queries<12) await new Promise(r=>setTimeout(r,5000));
    }
    const groups={};
    for(const e of events){
      const w=e.$workers??{};
      if(w.truncated)throw Error('Truncated invocation event');
      const method=['Acquire','TryAcquire','Renew','Release','OpenSession','AcknowledgeSession','GetRequestStatus','CancelPending'].find(m=>w.event?.request?.url?.endsWith('/'+m))??'other';
      const kind=w.entrypoint==='PintleAuthority'?'authority':'gateway';
      const k=kind+'/'+(w.eventType==='alarm'?'alarm':method);
      const g=groups[k]??={events:0,cpu:[],wall:[],outcomes:{}};g.events++;
      for(const [field,list] of [['cpuTimeMs','cpu'],['wallTimeMs','wall']]){if(typeof w[field]==='number'&&Number.isFinite(w[field])&&w[field]>=0)g[list].push(w[field]);}
      const outcome=['ok','canceled','exception','exceededCpu','exceededMemory'].includes(w.outcome)?w.outcome:'other';g.outcomes[outcome]=(g.outcomes[outcome]??0)+1;
    }
    function summary(a){a.sort((a,b)=>a-b);if(!a.length)return {samples:0};const q=p=>a[Math.ceil(a.length*p)-1];return {samples:a.length,sum_ms:a.reduce((a,b)=>a+b,0),p50_ms:q(.5),p95_ms:q(.95),p99_ms:q(.99),max_ms:a.at(-1)};}
    const report={started_at:run.started_at,finished_at:run.finished_at,queries,observed_gateway_events:events.filter(e=>e.$workers?.entrypoint!=='PintleAuthority').length,expected_client_sends:run.rpc_sent,scope:'persisted invocation CPU/wall time, not billed duration or SQL usage',groups:Object.fromEntries(Object.entries(groups).map(([k,g])=>[k,{events:g.events,cpu:summary(g.cpu),wall:summary(g.wall),outcomes:g.outcomes}]))};
    writeFileSync('test-results/staging-capacity-metrics.json',JSON.stringify(report,null,2)+'\n',{mode:0o600});
    console.log(JSON.stringify(report,null,2));
    JS
