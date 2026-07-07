//! Run an external script or executable and get its result as data.
//!
//! [`Script`] takes a program (+ args + env), runs it, captures stdout/stderr,
//! and maps the outcome to a typed [`Result`]: a zero exit yields
//! [`ScriptOutput`]; a missing program, a spawn failure, or a non-zero exit
//! each become a distinct [`ScriptError`] (the failure carries stderr, so the
//! reason survives to logs and UIs).
//!
//! It is **cross-platform and shell-agnostic** — it drives
//! [`std::process::Command`] and assumes nothing about bash or a shell. Point
//! [`Script::new`] at any directly-runnable executable (a shebang'd `.sh` on
//! Unix; a `.ps1`/`.cmd`/`.exe` on Windows), or use [`Script::with_interpreter`]
//! to run a script through an interpreter (`bash foo.sh`, `pwsh -File foo.ps1`)
//! when it isn't directly executable.
//!
//! ```no_run
//! use allex_script::Script;
//! let out = Script::new("/usr/libexec/app/configure.sh")
//!     .arg("--apply")
//!     .env("APP_POLICY", "strict")
//!     .run()?;
//! println!("{}", out.stdout);
//! # Ok::<(), allex_script::ScriptError>(())
//! ```

use std::ffi::{OsStr, OsString};
use std::process::{Command, ExitStatus};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ScriptError {
    /// The program could not be located (e.g. not installed, or not on PATH).
    #[error("executable not found: {0:?}")]
    NotFound(OsString),
    /// The program was found but could not be launched.
    #[error("failed to launch {program:?}")]
    Spawn {
        program: OsString,
        #[source]
        source: std::io::Error,
    },
    /// The program ran and exited non-zero. `stderr` is trimmed and retained.
    #[error("{program:?} exited with {status}: {stderr}")]
    Failed {
        program: OsString,
        status: ExitStatus,
        stderr: String,
    },
}

/// Captured result of a successful run.
#[derive(Debug, Clone)]
pub struct ScriptOutput {
    pub stdout: String,
    pub stderr: String,
}

/// A program to run: the executable (+ optional leading interpreter script),
/// positional args, and environment overrides. Build with the chained setters
/// and finish with [`Script::run`].
#[derive(Debug, Clone)]
pub struct Script {
    program: OsString,
    args: Vec<OsString>,
    envs: Vec<(OsString, OsString)>,
}

impl Script {
    /// Run `program` directly (a directly-executable script or binary).
    pub fn new(program: impl Into<OsString>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            envs: Vec::new(),
        }
    }

    /// Run `script` through `interpreter` — the interpreter becomes the program
    /// and the script its first argument (`bash foo.sh`, `pwsh -File foo.ps1`,
    /// the latter via `.with_interpreter("pwsh", "-File").arg("foo.ps1")`).
    pub fn with_interpreter(interpreter: impl Into<OsString>, script: impl Into<OsString>) -> Self {
        Self {
            program: interpreter.into(),
            args: vec![script.into()],
            envs: Vec::new(),
        }
    }

    /// Append a positional argument.
    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Append several positional arguments.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Set an environment variable for the child.
    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.envs.push((key.into(), value.into()));
        self
    }

    /// The program this will invoke.
    pub fn program(&self) -> &OsStr {
        &self.program
    }

    /// Run the program, capturing output. See [`ScriptError`] for the failure
    /// modes.
    pub fn run(self) -> Result<ScriptOutput, ScriptError> {
        let output = Command::new(&self.program)
            .args(&self.args)
            .envs(self.envs.iter().map(|(k, v)| (k, v)))
            .output();

        let output = match output {
            Ok(output) => output,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Err(ScriptError::NotFound(self.program));
            }
            Err(source) => {
                return Err(ScriptError::Spawn {
                    program: self.program,
                    source,
                });
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if !output.status.success() {
            return Err(ScriptError::Failed {
                program: self.program,
                status: output.status,
                stderr: stderr.trim().to_string(),
            });
        }
        Ok(ScriptOutput { stdout, stderr })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_program_maps_to_not_found() {
        let err = Script::new("allex-script-definitely-missing-xyz")
            .run()
            .unwrap_err();
        assert!(matches!(err, ScriptError::NotFound(_)));
    }

    #[cfg(unix)]
    #[test]
    fn success_captures_stdout_and_passes_args() {
        let out = Script::new("sh")
            .arg("-c")
            .arg(r#"printf 'hi %s' "$1""#)
            .arg("sh") // $0
            .arg("alice") // $1
            .run()
            .unwrap();
        assert_eq!(out.stdout, "hi alice");
    }

    #[cfg(unix)]
    #[test]
    fn env_is_passed_to_the_child() {
        let out = Script::new("sh")
            .arg("-c")
            .arg(r#"printf '%s' "$DS_POLICY""#)
            .env("DS_POLICY", "strict")
            .run()
            .unwrap();
        assert_eq!(out.stdout, "strict");
    }

    #[cfg(unix)]
    #[test]
    fn nonzero_exit_carries_status_and_stderr() {
        let err = Script::new("sh")
            .arg("-c")
            .arg("echo boom >&2; exit 3")
            .run()
            .unwrap_err();
        match err {
            ScriptError::Failed { status, stderr, .. } => {
                assert_eq!(status.code(), Some(3));
                assert_eq!(stderr, "boom");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }
}
