use dashmap::DashMap;
use std::sync::Arc;
use async_trait::async_trait;

use transport::{Payload, Result, Error};
use crate::{MethodExecutor, ModuleExecutor, RelayedModuleExecutor};

pub struct Registry {
    servers: DashMap<String, Arc<dyn ModuleExecutor>>,
    modules: DashMap<String, Arc<dyn MethodExecutor>>,
}

impl Registry {
    pub fn new() -> Self {
        Self { servers: DashMap::new(), modules: DashMap::new() }
    }

    pub fn register_server(&self, name: &str, executor: Arc<dyn ModuleExecutor>) {
        self.servers.insert(name.to_string(), executor);
    }

    pub fn register_module (&self, module: Arc<dyn MethodExecutor>) {
        self.modules.insert(module.get_name().to_string(), module);
    }
}

#[async_trait]
impl ModuleExecutor for Registry {
    async fn execute(&self, module: &str, method: &str, payload: Payload) -> Result<Payload> {
        match self.modules.get(module) {
            Some(executor) => executor.execute(method, payload).await,
            None => Err(Error::ModuleNotFound),
        }
    }
}

#[async_trait]
impl RelayedModuleExecutor for Registry {
    async fn execute(&self, server: &str, module: &str, method: &str, payload: Payload) -> Result<Payload> {
        match self.servers.get(server) {
            Some(executor) => executor.execute(module, method, payload).await,
            None => Err(Error::ServerNotFound),
        }
    }
}