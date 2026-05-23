use dashmap::DashMap;
use std::sync::Arc;
use async_trait::async_trait;

use transport::{Payload, Result, Error};
use crate::{Executor, ModuleExecutor, RelayedModuleExecutor};

pub struct ModuleRegistry {
    modules: DashMap<String, Arc<dyn Executor>>,
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self { modules: DashMap::new() }
    }

    pub fn register(&self, module: Arc<dyn Executor>) {
        self.modules.insert(module.get_name().to_string(), module);
    }
}

#[async_trait]
impl ModuleExecutor for ModuleRegistry {
    async fn execute(&self, module: &str, method: &str, payload: Payload) -> Result<Payload> {
        match self.modules.get(module) {
            Some(executor) => executor.execute(method, payload).await,
            None => Err(Error::ModuleNotFound),
        }
    }
}

pub struct ServerRegistry {
    servers: DashMap<String, Arc<dyn ModuleExecutor>>,
}

impl ServerRegistry {
    pub fn new() -> Self {
        Self { servers: DashMap::new() }
    }

    pub fn register(&self, name: &str, executor: Arc<dyn ModuleExecutor>) {
        self.servers.insert(name.to_string(), executor);
    }
}

#[async_trait]
impl RelayedModuleExecutor for ServerRegistry {
    async fn execute(&self, server: &str, module: &str, method: &str, payload: Payload) -> Result<Payload> {
        match self.servers.get(server) {
            Some(executor) => executor.execute(module, method, payload).await,
            None => Err(Error::ServerNotFound),
        }
    }
}