//! Process-startup boilerplate every allex backend re-writes: JWT-secret loading (with a
//! persisted dev fallback), TTL/env parsing, `.env` loading, and bind-address resolution.
//!
//! # Why this exists
//!
//! Two audited backends had **byte-identical** `config.rs` files and a near-identical `main.rs`
//! prologue — only the app name and an env-var prefix differed. That is boilerplate that should be
//! code, not a copy-paste template. This module owns it once, correctly.
//!
//! # Two flavours of helper
//!
//! - **App-agnostic free functions** — [`load_env_u64`], [`load_jwt_ttl_secs`],
//!   [`load_refresh_token_ttl_secs`], [`resolve_bind_address`]. They read fixed, conventional env
//!   vars and need no per-app identity.
//! - **[`Bootstrap`]** — carries the two per-app identifiers ([`state_dir`](Bootstrap::new) for the
//!   persisted dev-secret directory, and an uppercase `env_prefix` for app-scoped overrides) and
//!   exposes [`load_dotenv`](Bootstrap::load_dotenv) + [`load_jwt_secret`](Bootstrap::load_jwt_secret).
//!
//! ```
//! use allex_host::bootstrap::{Bootstrap, load_jwt_ttl_secs, resolve_bind_address};
//!
//! let app = Bootstrap::new("myapp-backend", "MYAPP");
//! app.load_dotenv(); // reads $MYAPP_ENV_FILE or ./.env; real env always wins
//! let jwt_secret = app.load_jwt_secret().expect("JWT secret");
//! let jwt_ttl = load_jwt_ttl_secs().expect("valid JWT_TTL_SECS");
//! let address = resolve_bind_address(3300); // CLI arg 1 / $PORT / $BIND_ADDRESS
//! # let _ = (jwt_secret, jwt_ttl, address);
//! ```

use std::env;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Conventional default refresh-token lifetime: 7 days, in seconds. Callers use this when
/// [`load_refresh_token_ttl_secs`] returns `None`.
pub const DEFAULT_REFRESH_TOKEN_TTL_SECS: u64 = 604_800;

/// Reads an optional positive-integer value from environment variable `var`.
///
/// Returns `Ok(None)` when the variable is absent, `Ok(Some(n))` when it parses, and an error when
/// it is present but not a valid `u64` (whitespace is trimmed first).
pub fn load_env_u64(var: &str) -> Result<Option<u64>, Box<dyn Error>> {
    match env::var(var) {
        Err(_) => Ok(None),
        Ok(val) => val
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("{var} must be a positive integer, got: {val}").into()),
    }
}

/// Reads `JWT_TTL_SECS` (access-token lifetime override, in seconds). See [`load_env_u64`].
pub fn load_jwt_ttl_secs() -> Result<Option<u64>, Box<dyn Error>> {
    load_env_u64("JWT_TTL_SECS")
}

/// Reads `REFRESH_TOKEN_TTL_SECS` (refresh-token lifetime override, in seconds); callers fall back
/// to [`DEFAULT_REFRESH_TOKEN_TTL_SECS`] when this is `None`. See [`load_env_u64`].
pub fn load_refresh_token_ttl_secs() -> Result<Option<u64>, Box<dyn Error>> {
    load_env_u64("REFRESH_TOKEN_TTL_SECS")
}

/// Prints a warning for any short-lived-token override currently in effect. Short TTLs are a
/// testing aid; logging them on startup makes an accidental production override visible.
pub fn warn_ttl_overrides(jwt_ttl_secs: Option<u64>, refresh_token_ttl_secs: Option<u64>) {
    if let Some(ttl) = jwt_ttl_secs {
        println!("[WARN] JWT_TTL_SECS={ttl} -- short-lived access tokens enabled (testing only)");
    }
    if let Some(ttl) = refresh_token_ttl_secs {
        println!(
            "[WARN] REFRESH_TOKEN_TTL_SECS={ttl} -- short-lived refresh tokens enabled (testing only)"
        );
    }
}

/// Resolves the address to bind the HTTP listener to.
///
/// `$BIND_ADDRESS` wins outright if set. Otherwise the port is taken from the first CLI positional
/// argument, then `$PORT`, then `default_port`, and the address is `127.0.0.1:<port>`.
pub fn resolve_bind_address(default_port: u16) -> String {
    if let Ok(address) = env::var("BIND_ADDRESS") {
        return address;
    }
    let port = env::args()
        .nth(1)
        .or_else(|| env::var("PORT").ok())
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default_port);
    format!("127.0.0.1:{port}")
}

/// Per-app identity for the env-scoped bootstrap helpers.
///
/// Holds two identifiers the app-agnostic free functions don't need:
/// - `state_dir` — the directory name under the OS state dir where a dev JWT secret is persisted
///   (e.g. `"myapp-backend"`).
/// - `env_prefix` — an uppercase prefix for app-specific override vars, e.g. `"MYAPP"` yields
///   `MYAPP_ENV_FILE` and `MYAPP_DEV_JWT_SECRET_PATH`.
pub struct Bootstrap {
    state_dir: String,
    env_prefix: String,
}

impl Bootstrap {
    /// Creates a bootstrap for an app persisting dev artifacts under `state_dir` and reading
    /// app-scoped overrides prefixed with `env_prefix` (conventionally uppercase).
    pub fn new(state_dir: impl Into<String>, env_prefix: impl Into<String>) -> Self {
        Self {
            state_dir: state_dir.into(),
            env_prefix: env_prefix.into(),
        }
    }

    /// Loads a `.env` file for local development into the process environment. Reads the file named
    /// by `$<PREFIX>_ENV_FILE` if set, otherwise `./.env`. A missing file is fine; values already
    /// present in the environment are never overwritten, so container deployments are unaffected.
    pub fn load_dotenv(&self) {
        let env_file_var = format!("{}_ENV_FILE", self.env_prefix);
        let result = match env::var(&env_file_var) {
            Ok(path) => dotenvy::from_path(&path).map(|_| path),
            Err(_) => dotenvy::dotenv().map(|p| p.display().to_string()),
        };
        match result {
            Ok(path) => println!("[config] loaded environment from {path}"),
            Err(e) if e.not_found() => {}
            Err(e) => eprintln!("[config] could not load env file: {e}"),
        }
    }

    /// Loads the JWT signing secret.
    ///
    /// `$JWT_SECRET` is authoritative when set (and rejected if empty or non-UTF-8). When it is
    /// unset, debug builds fall back to a 256-bit secret persisted under the OS state directory
    /// (created on first run, reused thereafter) so local dev tokens survive restarts; release
    /// builds require `$JWT_SECRET` and error otherwise.
    pub fn load_jwt_secret(&self) -> Result<String, Box<dyn Error>> {
        match env::var("JWT_SECRET") {
            Ok(secret) if !secret.trim().is_empty() => Ok(secret),
            Ok(_) => Err("JWT_SECRET environment variable cannot be empty".into()),
            Err(env::VarError::NotPresent) => self.load_or_create_dev_jwt_secret(),
            Err(env::VarError::NotUnicode(_)) => {
                Err("JWT_SECRET environment variable must be valid UTF-8".into())
            }
        }
    }

    #[cfg(debug_assertions)]
    fn load_or_create_dev_jwt_secret(&self) -> Result<String, Box<dyn Error>> {
        let secret_path = self.dev_jwt_secret_path()?;

        if let Ok(secret) = fs::read_to_string(&secret_path) {
            let secret = secret.trim().to_owned();
            if !secret.is_empty() {
                return Ok(secret);
            }
        }

        let secret = generate_secret();
        write_secret_file(&secret_path, &secret)?;
        Ok(secret)
    }

    #[cfg(not(debug_assertions))]
    fn load_or_create_dev_jwt_secret(&self) -> Result<String, Box<dyn Error>> {
        Err("JWT_SECRET environment variable is required".into())
    }

    /// Path of the persisted dev secret: `$<PREFIX>_DEV_JWT_SECRET_PATH` if set, otherwise
    /// `<state_dir>/dev-jwt-secret` under the first available OS state directory.
    #[cfg(debug_assertions)]
    fn dev_jwt_secret_path(&self) -> Result<PathBuf, io::Error> {
        let override_var = format!("{}_DEV_JWT_SECRET_PATH", self.env_prefix);
        if let Some(path) = env::var_os(&override_var) {
            return Ok(PathBuf::from(path));
        }

        if let Some(path) = env::var_os("XDG_STATE_HOME") {
            return Ok(PathBuf::from(path)
                .join(&self.state_dir)
                .join("dev-jwt-secret"));
        }

        // Windows: prefer %LOCALAPPDATA% (e.g. C:\Users\<name>\AppData\Local).
        if let Some(path) = env::var_os("LOCALAPPDATA") {
            return Ok(PathBuf::from(path)
                .join(&self.state_dir)
                .join("dev-jwt-secret"));
        }

        if let Some(path) = env::var_os("HOME") {
            return Ok(PathBuf::from(path)
                .join(".local")
                .join("state")
                .join(&self.state_dir)
                .join("dev-jwt-secret"));
        }

        // Windows fallback: %USERPROFILE%.
        if let Some(path) = env::var_os("USERPROFILE") {
            return Ok(PathBuf::from(path)
                .join(".local")
                .join("state")
                .join(&self.state_dir)
                .join("dev-jwt-secret"));
        }

        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "JWT_SECRET is unset and no state directory is available for a persisted dev secret",
        ))
    }
}

/// 256 bits of CSPRNG entropy (two v4 UUIDs), hex-encoded without dashes — a 64-char secret.
#[cfg(debug_assertions)]
fn generate_secret() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Writes `secret` to `path` (creating parent dirs), truncating any existing file, mode `0o600` on
/// unix so other users can't read the dev secret.
#[cfg(debug_assertions)]
fn write_secret_file(path: &Path, secret: &str) -> Result<(), io::Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut options = OpenOptions::new();
    options.write(true).truncate(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }

    let mut file = options.open(path)?;
    file.write_all(secret.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    // Tests mutate process-global env; serialize them.
    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvGuard {
        path: PathBuf,
        jwt_secret: Option<String>,
        secret_path: Option<String>,
    }

    impl EnvGuard {
        fn new(path: PathBuf) -> Self {
            Self {
                path,
                jwt_secret: env::var("JWT_SECRET").ok(),
                secret_path: env::var("TESTAPP_DEV_JWT_SECRET_PATH").ok(),
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            restore_var("JWT_SECRET", self.jwt_secret.as_deref());
            restore_var("TESTAPP_DEV_JWT_SECRET_PATH", self.secret_path.as_deref());
            let _ = fs::remove_file(&self.path);
            if let Some(parent) = self.path.parent() {
                let _ = fs::remove_dir(parent);
            }
        }
    }

    fn restore_var(key: &str, value: Option<&str>) {
        match value {
            Some(value) => unsafe { env::set_var(key, value) },
            None => unsafe { env::remove_var(key) },
        }
    }

    fn unique_secret_path() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        env::temp_dir()
            .join(format!("allex-host-test-{suffix}"))
            .join("dev-jwt-secret")
    }

    fn configure_test_env(path: &Path) -> EnvGuard {
        let guard = EnvGuard::new(path.to_path_buf());
        unsafe {
            env::remove_var("JWT_SECRET");
            env::set_var("TESTAPP_DEV_JWT_SECRET_PATH", path);
        }
        guard
    }

    fn app() -> Bootstrap {
        Bootstrap::new("testapp-backend", "TESTAPP")
    }

    #[test]
    fn prefers_explicit_jwt_secret() {
        let _env_guard = env_lock().lock().unwrap();
        let path = unique_secret_path();
        let _guard = configure_test_env(&path);
        unsafe { env::set_var("JWT_SECRET", "from-env") };

        let secret = app().load_jwt_secret().unwrap();

        assert_eq!(secret, "from-env");
        assert!(!path.exists());
    }

    #[test]
    fn rejects_empty_jwt_secret() {
        let _env_guard = env_lock().lock().unwrap();
        let path = unique_secret_path();
        let _guard = configure_test_env(&path);
        unsafe { env::set_var("JWT_SECRET", "   ") };

        assert!(app().load_jwt_secret().is_err());
    }

    #[test]
    fn persists_dev_secret_across_reloads() {
        let _env_guard = env_lock().lock().unwrap();
        let path = unique_secret_path();
        let _guard = configure_test_env(&path);

        let first = app().load_jwt_secret().unwrap();
        let second = app().load_jwt_secret().unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), first);
    }

    #[test]
    fn load_env_u64_parses_and_rejects() {
        let _env_guard = env_lock().lock().unwrap();
        let var = "ALLEX_HOST_TEST_TTL";
        unsafe { env::remove_var(var) };
        assert_eq!(load_env_u64(var).unwrap(), None);
        unsafe { env::set_var(var, " 42 ") };
        assert_eq!(load_env_u64(var).unwrap(), Some(42));
        unsafe { env::set_var(var, "nope") };
        assert!(load_env_u64(var).is_err());
        unsafe { env::remove_var(var) };
    }
}
