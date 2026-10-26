# Rustup and Cargo caches belong to this project, not the host toolchain.
env = {
  RUSTUP_HOME: "${HERMIT_ENV}/.hermit/rustup",
  GOPATH: "${HERMIT_ENV}/.hermit/go",
  GOCACHE: "${HERMIT_ENV}/.hermit/go/cache",
  GOENV: "off",
  XDG_CONFIG_HOME: "${HERMIT_ENV}/.hermit/config",
  WRANGLER_SEND_METRICS: "false",
  BUF_CACHE_DIR: "${HERMIT_ENV}/.hermit/buf/cache"
}
