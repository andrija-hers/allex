use async_trait::async_trait;
use transport::{Payload, Result};

#[async_trait]
pub trait Executor: Send + Sync {
    fn get_name(&self) -> &str;
    async fn execute(&self, method: &str, payload: Payload) -> Result<Payload>;
}

#[async_trait]
pub trait ModuleExecutor: Send + Sync {
    async fn execute(&self, module: &str, method: &str, payload: Payload) -> Result<Payload>;
}

#[async_trait]
pub trait RelayedModuleExecutor: Send + Sync {
    async fn execute(&self, server: &str, module: &str, method: &str, payload: Payload) -> Result<Payload>;
}

mod registry;
pub use registry::*;
