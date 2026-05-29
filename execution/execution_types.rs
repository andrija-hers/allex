use async_trait::async_trait;
use transport::{Payload, Result};

#[async_trait]
pub trait MethodExecutor: Send + Sync {
    fn get_name(&self) -> &str;
    async fn execute(&self, method: &str, payload: Payload, caller_role: Option<&str>) -> Result<Payload>;
}

#[async_trait]
pub trait ModuleExecutor: Send + Sync {
    async fn execute(&self, module: &str, method: &str, payload: Payload, caller_role: Option<&str>) -> Result<Payload>;
}

#[async_trait]
pub trait RelayedModuleExecutor: Send + Sync {
    async fn execute(
        &self,
        server: &str,
        module: &str,
        method: &str,
        payload: Payload,
        caller_role: Option<&str>,
    ) -> Result<Payload>;
}
