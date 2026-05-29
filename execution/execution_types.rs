use async_trait::async_trait;
use transport::{Payload, Result};

/// Executes methods within a single module after transport decoding is complete.
#[async_trait]
pub trait MethodExecutor: Send + Sync {
    /// Returns the wire-visible module name handled by this executor.
    fn get_name(&self) -> &str;
    /// Executes a method with the provided serialized payload and optional caller role.
    async fn execute(
        &self,
        method: &str,
        payload: Payload,
        caller_role: Option<&str>,
    ) -> Result<Payload>;
}

/// Routes calls to the correct module executor inside a single server.
#[async_trait]
pub trait ModuleExecutor: Send + Sync {
    /// Executes the named module method with the provided serialized payload.
    async fn execute(
        &self,
        module: &str,
        method: &str,
        payload: Payload,
        caller_role: Option<&str>,
    ) -> Result<Payload>;
}

/// Routes calls to a module executor hosted by another named server.
#[async_trait]
pub trait RelayedModuleExecutor: Send + Sync {
    /// Executes the named method on the named module hosted by the named server.
    async fn execute(
        &self,
        server: &str,
        module: &str,
        method: &str,
        payload: Payload,
        caller_role: Option<&str>,
    ) -> Result<Payload>;
}
