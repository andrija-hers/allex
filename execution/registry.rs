use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;

use crate::{
    MethodExecutor, ModuleExecutor, RelayedModuleExecutor, ResponseStream, StreamingMethodExecutor,
};
use transport::tracing::{debug, debug_span, warn};
use transport::{Error, Payload, Result};

/// In-memory registry of module executors and relayed server executors.
pub struct Registry {
    servers: DashMap<String, Arc<dyn ModuleExecutor>>,
    modules: DashMap<String, Arc<dyn MethodExecutor>>,
    streaming: DashMap<String, Arc<dyn StreamingMethodExecutor>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            servers: DashMap::new(),
            modules: DashMap::new(),
            streaming: DashMap::new(),
        }
    }

    /// Registers a named upstream server executor for relayed calls.
    pub fn register_server(&self, name: &str, executor: Arc<dyn ModuleExecutor>) {
        self.servers.insert(name.to_string(), executor);
    }

    /// Registers a local module executor (unary methods) keyed by module name.
    pub fn register_module(&self, module: Arc<dyn MethodExecutor>) {
        self.modules.insert(module.get_name().to_string(), module);
    }

    /// Registers a module's streaming methods keyed by module name.
    pub fn register_streaming_module(&self, module: Arc<dyn StreamingMethodExecutor>) {
        self.streaming.insert(module.get_name().to_string(), module);
    }

    /// Registers a generated responder for BOTH unary and streaming dispatch in
    /// one call. Generated `<Trait>MethodResponder`s implement both traits, so
    /// this is the usual registration entry point.
    pub fn register<M>(&self, module: Arc<M>)
    where
        M: MethodExecutor + StreamingMethodExecutor + 'static,
    {
        self.modules.insert(
            MethodExecutor::get_name(&*module).to_string(),
            module.clone(),
        );
        self.streaming.insert(
            StreamingMethodExecutor::get_name(&*module).to_string(),
            module,
        );
    }

    /// Begins a streaming method on a registered module, or returns `None` if the
    /// module/method isn't a streaming endpoint (the caller should try unary).
    pub fn execute_streaming(
        &self,
        module: &str,
        method: &str,
        payload: Payload,
        caller_role: Option<&str>,
    ) -> Option<ResponseStream> {
        self.streaming
            .get(module)
            .and_then(|m| m.execute_streaming(method, payload, caller_role))
    }
}

#[async_trait]
impl ModuleExecutor for Registry {
    async fn execute(
        &self,
        module: &str,
        method: &str,
        payload: Payload,
        caller_role: Option<&str>,
    ) -> Result<Payload> {
        let payload_bytes = payload.len();
        let caller_role = caller_role.unwrap_or("anonymous");
        let span = debug_span!(
            "allex.registry.module_execute",
            rpc_module = module,
            rpc_method = method,
            payload_bytes,
            caller_role,
        );
        let _guard = span.enter();
        match self.modules.get(module) {
            Some(executor) => {
                debug!("dispatching to registered module executor");
                executor.execute(method, payload, Some(caller_role)).await
            }
            None => {
                warn!("module executor not found");
                Err(Error::ModuleNotFound)
            }
        }
    }
}

#[async_trait]
impl RelayedModuleExecutor for Registry {
    async fn execute(
        &self,
        server: &str,
        module: &str,
        method: &str,
        payload: Payload,
        caller_role: Option<&str>,
    ) -> Result<Payload> {
        let payload_bytes = payload.len();
        let caller_role = caller_role.unwrap_or("anonymous");
        let span = debug_span!(
            "allex.registry.relayed_execute",
            rpc_server = server,
            rpc_module = module,
            rpc_method = method,
            payload_bytes,
            caller_role,
        );
        let _guard = span.enter();
        match self.servers.get(server) {
            Some(executor) => {
                debug!("dispatching to registered upstream server executor");
                executor
                    .execute(module, method, payload, Some(caller_role))
                    .await
            }
            None => {
                warn!("upstream server executor not found");
                Err(Error::ServerNotFound)
            }
        }
    }
}
