//! allex-winsvc — a thin, reusable **Windows-service host harness**: the SCM control handler + status
//! transitions, and `install`/`uninstall` with a Service Recovery policy. Nothing app-specific lives
//! here — an app supplies its [`ServiceSpec`] and a service **body** (its config/logging/runtime), and
//! reuses this glue.
//!
//! The Windows SCM entry point must be a plain `extern` callback, so the app still writes the one line
//! `windows_service::define_windows_service!(ffi_main, service_main)` plus a tiny `service_main` that
//! calls [`run_service_body`] with its body closure. See the EpicFIX `fix-svc-host` for the pattern.
//!
//! On non-Windows this crate is empty, so a cross-platform workspace can depend on it unconditionally.

#![cfg(windows)]

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub use tokio::sync::Notify;
use windows_service::service::{
    ServiceAccess, ServiceAction, ServiceActionType, ServiceControl, ServiceControlAccept,
    ServiceErrorControl, ServiceExitCode, ServiceFailureActions, ServiceFailureResetPeriod,
    ServiceInfo, ServiceStartType, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

const SERVICE_TYPE: ServiceType = ServiceType::OWN_PROCESS;

/// Identity of the service in the SCM.
pub struct ServiceSpec {
    /// Service key name (used by the SCM + as the Event Log source).
    pub name: String,
    /// Human-facing name shown in the Services console.
    pub display_name: String,
    /// One-line description.
    pub description: String,
}

/// How a service body run ended — drives the SCM exit code and whether Service Recovery restarts.
pub enum Outcome {
    /// A clean stop, or a *deliberate* refusal to start (e.g. an operator must intervene). The SCM
    /// records `Win32(0)` and does **not** auto-restart.
    Clean,
    /// A failure — recorded as `ServiceSpecific(1)` so Service Recovery restarts with backoff.
    Failed,
}

/// Service Recovery (failure-action) policy. [`Default`] is restart-with-backoff 10s/60s/300s, a 1-hour
/// reset window, and recovery on non-crash (clean-but-error) exits too.
pub struct RecoveryPolicy {
    /// Restart delays applied in order per successive failure; the last repeats (so no hot-loop).
    pub restart_delays: Vec<Duration>,
    /// Time without a failure after which the failure counter resets to zero.
    pub reset_period: Duration,
    /// Also recover when the service stops with a non-zero exit that isn't an access-violation crash.
    pub recover_on_non_crash: bool,
}

impl Default for RecoveryPolicy {
    fn default() -> Self {
        Self {
            restart_delays: vec![
                Duration::from_secs(10),
                Duration::from_secs(60),
                Duration::from_secs(300),
            ],
            reset_period: Duration::from_secs(3600),
            recover_on_non_crash: true,
        }
    }
}

/// Run a service body under the SCM: register the Stop/Shutdown control handler, mark the service
/// **Running**, run `body` (handing it a shutdown [`Notify`] to await — `notify_one`, which *stores* a
/// permit so a Stop arriving before the body awaits isn't lost), then mark it **Stopped** with the exit
/// code implied by the body's [`Outcome`]. Call this from your `service_main`.
pub fn run_service_body<F>(spec: &ServiceSpec, body: F) -> windows_service::Result<()>
where
    F: FnOnce(Arc<Notify>) -> Outcome,
{
    let notify = Arc::new(Notify::new());
    let handler_notify = notify.clone();
    let event_handler = move |control: ServiceControl| -> ServiceControlHandlerResult {
        match control {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                handler_notify.notify_one();
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    };
    let status_handle = service_control_handler::register(&spec.name, event_handler)?;

    let set = |state: ServiceState, controls: ServiceControlAccept, exit: ServiceExitCode| {
        status_handle.set_service_status(ServiceStatus {
            service_type: SERVICE_TYPE,
            current_state: state,
            controls_accepted: controls,
            exit_code: exit,
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
        })
    };

    set(
        ServiceState::Running,
        ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
        ServiceExitCode::Win32(0),
    )?;

    let exit = match body(notify) {
        Outcome::Clean => ServiceExitCode::Win32(0),
        Outcome::Failed => ServiceExitCode::ServiceSpecific(1),
    };
    set(ServiceState::Stopped, ServiceControlAccept::empty(), exit)?;
    Ok(())
}

/// Install an auto-start service running `exe` with `launch_args`, under `account` (LocalSystem if
/// `None`; prefer a low-privilege virtual account / gMSA), and apply the Service Recovery `recovery`.
/// Run elevated.
pub fn install(
    spec: &ServiceSpec,
    exe: PathBuf,
    launch_args: Vec<OsString>,
    account: Option<String>,
    password: Option<String>,
    recovery: &RecoveryPolicy,
) -> windows_service::Result<()> {
    let manager = ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    )?;

    let info = ServiceInfo {
        name: OsString::from(&spec.name),
        display_name: OsString::from(&spec.display_name),
        service_type: SERVICE_TYPE,
        start_type: ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: exe,
        launch_arguments: launch_args,
        dependencies: vec![],
        account_name: account.map(OsString::from),
        account_password: password.map(OsString::from),
    };

    let service =
        manager.create_service(&info, ServiceAccess::CHANGE_CONFIG | ServiceAccess::START)?;
    service.set_description(&spec.description)?;

    let actions = recovery
        .restart_delays
        .iter()
        .map(|d| ServiceAction {
            action_type: ServiceActionType::Restart,
            delay: *d,
        })
        .collect();
    service.update_failure_actions(ServiceFailureActions {
        reset_period: ServiceFailureResetPeriod::After(recovery.reset_period),
        reboot_msg: None,
        command: None,
        actions: Some(actions),
    })?;
    if recovery.recover_on_non_crash {
        service.set_failure_actions_on_non_crash_failures(true)?;
    }
    Ok(())
}

/// Stop (best-effort) and delete a service by name. Run elevated.
pub fn uninstall(name: &str) -> windows_service::Result<()> {
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
    let service = manager.open_service(
        name,
        ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
    )?;
    let _ = service.stop();
    service.delete()?;
    Ok(())
}
