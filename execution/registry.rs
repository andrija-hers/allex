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
        // "anonymous" is a LABEL for the span, never a role. Passing it on as
        // `Some("anonymous")` would tell the permission gate that an
        // unauthenticated request has an identity — which every `#[authenticated]`
        // method would then accept, since that tier takes any role. The caller
        // must reach the gate exactly as it arrived.
        let caller_label = caller_role.unwrap_or("anonymous");
        let span = debug_span!(
            "allex.registry.module_execute",
            rpc_module = module,
            rpc_method = method,
            payload_bytes,
            caller_role = caller_label,
        );
        let _guard = span.enter();
        match self.modules.get(module) {
            Some(executor) => {
                debug!("dispatching to registered module executor");
                executor.execute(method, payload, caller_role).await
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
        // See the note in `ModuleExecutor::execute`: a label, not a role.
        let caller_label = caller_role.unwrap_or("anonymous");
        let span = debug_span!(
            "allex.registry.relayed_execute",
            rpc_server = server,
            rpc_module = module,
            rpc_method = method,
            payload_bytes,
            caller_role = caller_label,
        );
        let _guard = span.enter();
        match self.servers.get(server) {
            Some(executor) => {
                debug!("dispatching to registered upstream server executor");
                executor.execute(module, method, payload, caller_role).await
            }
            None => {
                warn!("upstream server executor not found");
                Err(Error::ServerNotFound)
            }
        }
    }
}

#[cfg(test)]
mod caller_passthrough_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Records the `caller_role` its `execute` was handed, so a test can assert
    /// what the registry actually passed rather than what it logged.
    struct Recorder(Arc<Mutex<Option<Option<String>>>>);

    #[async_trait]
    impl MethodExecutor for Recorder {
        fn get_name(&self) -> &str {
            "recorder_v_1"
        }

        async fn execute(
            &self,
            _method: &str,
            _payload: Payload,
            caller_role: Option<&str>,
        ) -> Result<Payload> {
            *self.0.lock().expect("not poisoned") = Some(caller_role.map(|role| role.to_owned()));
            Ok(Payload::new(b"{}"))
        }
    }

    /// The registry logs an absent caller as "anonymous". It must not *pass* that
    /// on: a permission gate cannot tell an unauthenticated request from a
    /// caller whose role happens to be the string "anonymous", and every
    /// `#[authenticated]` method accepts any role — so collapsing the two made
    /// that whole tier callable without a token. Caught by a live two-node test,
    /// not by unit tests of the permission table, which is the lesson.
    #[test]
    fn an_absent_caller_stays_absent_through_dispatch() {
        let seen = Arc::new(Mutex::new(None));
        let registry = Registry::new();
        registry.register_module(Arc::new(Recorder(seen.clone())));

        futures::executor::block_on(ModuleExecutor::execute(
            &registry,
            "recorder_v_1",
            "anything",
            Payload::new(b"{}"),
            None,
        ))
        .expect("dispatch succeeds");

        assert_eq!(
            *seen.lock().expect("not poisoned"),
            Some(None),
            "the module executor must receive None, not Some(\"anonymous\")"
        );
    }

    #[test]
    fn a_present_caller_arrives_verbatim() {
        let seen = Arc::new(Mutex::new(None));
        let registry = Registry::new();
        registry.register_module(Arc::new(Recorder(seen.clone())));

        // The wire form, record separator and all — the gate parses it itself.
        futures::executor::block_on(ModuleExecutor::execute(
            &registry,
            "recorder_v_1",
            "anything",
            Payload::new(b"{}"),
            Some("andra\u{1e}user"),
        ))
        .expect("dispatch succeeds");

        assert_eq!(
            *seen.lock().expect("not poisoned"),
            Some(Some("andra\u{1e}user".to_owned()))
        );
    }
}
