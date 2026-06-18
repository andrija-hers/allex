use async_trait::async_trait;
use transport::{CallResponse, Payload, Result};

/// A boxed stream of transport-level call responses produced by a streaming
/// (server-push) RPC method. Same concrete type the `#[rpc_module]` macro emits.
pub type ResponseStream = std::pin::Pin<Box<dyn futures::Stream<Item = CallResponse> + Send>>;

/// Dispatches streaming RPC methods within a single module.
///
/// The generated `<Trait>MethodResponder` implements this alongside
/// [`MethodExecutor`], so streaming gets a trait object just like unary does and
/// can be routed generically (see the `allex-serve` crate). Returns `None` when
/// `method` is not a streaming method on this module — the caller should then
/// fall back to unary dispatch.
pub trait StreamingMethodExecutor: Send + Sync {
    /// Returns the wire-visible module name handled by this executor.
    fn get_name(&self) -> &str;

    /// Begins a streaming method, or returns `None` if `method` is not streaming.
    fn execute_streaming(
        &self,
        method: &str,
        payload: Payload,
        caller_role: Option<&str>,
    ) -> Option<ResponseStream>;
}

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
