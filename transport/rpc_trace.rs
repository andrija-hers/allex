//! The spans and events `#[rpc_module]` emits, as plain functions the generated code calls.
//!
//! Every `tracing` macro call expands to a static callsite, its field set and the enabled check:
//! about a hundred lines that the compiler type-checks and borrow-checks at each use. Expanded inside
//! every generated method (a span and up to six events per side), they made up most of the compile
//! time of a crate with a large `#[rpc_module]` trait. Here each callsite exists once, in this
//! crate, and the generated code only calls a function.
//!
//! The events keep their levels, messages and fields. Their `target` is [`CLIENT`] or [`SERVER`]
//! (the span names), not the module path of the crate holding the trait: a callsite here cannot
//! carry its caller's module path, so filter on `allex.rpc` (both sides) or one of the two.
//!
//! Not a stable API: only the code `#[rpc_module]` generates calls this.

use core::fmt::Debug;
use tracing::Span;

/// Target, and span name, of the client side.
pub const CLIENT: &str = "allex.rpc.client";
/// Target, and span name, of the server side.
pub const SERVER: &str = "allex.rpc.server";

// ── spans ───────────────────────────────────────────────────────────────────────────────────────

/// The span around one client call.
pub fn client_span(module: &'static str, method: &'static str, kind: &'static str) -> Span {
    tracing::debug_span!(
        target: CLIENT,
        "allex.rpc.client",
        rpc_module = module,
        rpc_method = method,
        rpc_kind = kind,
        rpc_side = "client",
    )
}

/// The span around one server call.
pub fn server_span(module: &'static str, method: &'static str, kind: &'static str) -> Span {
    tracing::debug_span!(
        target: SERVER,
        "allex.rpc.server",
        rpc_module = module,
        rpc_method = method,
        rpc_kind = kind,
        rpc_side = "server",
    )
}

// ── server, before the span ─────────────────────────────────────────────────────────────────────

/// A unary call refused by the role check.
pub fn server_denied(module: &'static str, method: &str, caller_role: Option<&str>) {
    tracing::warn!(
        target: SERVER,
        rpc_module = module,
        rpc_method = method,
        rpc_side = "server",
        outcome = "access_forbidden",
        caller_role = caller_role.unwrap_or("anonymous"),
        "rpc server authorization denied"
    );
}

/// A streaming call refused by the role check.
pub fn stream_server_denied(module: &'static str, method: &'static str, caller_role: Option<&str>) {
    tracing::warn!(
        target: SERVER,
        rpc_module = module,
        rpc_method = method,
        rpc_kind = "streaming",
        rpc_side = "server",
        outcome = "access_forbidden",
        caller_role = caller_role.unwrap_or("anonymous"),
        "rpc streaming server authorization denied"
    );
}

/// A unary request that did not decode.
pub fn unary_server_decode_failed(module: &'static str, method: &'static str, error: &dyn Debug) {
    tracing::warn!(
        target: SERVER,
        rpc_module = module,
        rpc_method = method,
        rpc_kind = "unary",
        rpc_side = "server",
        outcome = "decode_error",
        transport_error = ?error,
        "rpc unary server request decode failed"
    );
}

/// A streaming request that did not decode.
pub fn stream_server_decode_failed(module: &'static str, method: &'static str, error: &dyn Debug) {
    tracing::warn!(
        target: SERVER,
        rpc_module = module,
        rpc_method = method,
        rpc_kind = "streaming",
        rpc_side = "server",
        outcome = "decode_error",
        transport_error = ?error,
        "rpc streaming server request decode failed"
    );
}

// ── unary server (inside the entered span) ──────────────────────────────────────────────────────

pub fn unary_server_started() {
    tracing::debug!(target: SERVER, "rpc unary server call started");
}

pub fn unary_server_success() {
    tracing::debug!(target: SERVER, outcome = "success", "rpc unary server call finished");
}

pub fn unary_server_success_encode_failed(error: &dyn Debug) {
    tracing::warn!(target: SERVER, outcome = "encode_error", transport_error = ?error, "rpc unary server success encode failed");
}

pub fn unary_server_app_error() {
    tracing::debug!(target: SERVER, outcome = "app_error", "rpc unary server call finished");
}

pub fn unary_server_error_encode_failed(error: &dyn Debug) {
    tracing::warn!(target: SERVER, outcome = "encode_error", transport_error = ?error, "rpc unary server error encode failed");
}

// ── streaming server ────────────────────────────────────────────────────────────────────────────

pub fn stream_server_started(span: &Span) {
    tracing::debug!(target: SERVER, parent: span, "rpc streaming server call started");
}

pub fn stream_server_progress(span: &Span) {
    tracing::trace!(target: SERVER, parent: span, outcome = "progress", "rpc streaming server progress");
}

pub fn stream_server_success(span: &Span) {
    tracing::debug!(target: SERVER, parent: span, outcome = "success", "rpc streaming server call finished");
}

pub fn stream_server_app_error(span: &Span) {
    tracing::debug!(target: SERVER, parent: span, outcome = "app_error", "rpc streaming server call finished");
}

pub fn stream_server_interrupted(span: &Span) {
    tracing::warn!(target: SERVER, parent: span, outcome = "transport_interrupted", "rpc streaming server stream interrupted");
}

// ── unary client ────────────────────────────────────────────────────────────────────────────────

pub fn unary_client_success(span: &Span) {
    tracing::debug!(target: CLIENT, parent: span, outcome = "success", "rpc unary client call finished");
}

pub fn unary_client_app_error(span: &Span) {
    tracing::debug!(target: CLIENT, parent: span, outcome = "app_error", "rpc unary client call finished");
}

pub fn unary_client_interrupted(span: &Span) {
    tracing::warn!(target: CLIENT, parent: span, outcome = "transport_interrupted", "rpc unary client call interrupted");
}

pub fn unary_client_failed(span: &Span, error: &dyn Debug) {
    tracing::warn!(target: CLIENT, parent: span, outcome = "transport_error", transport_error = ?error, "rpc unary client call failed");
}

// ── streaming client ────────────────────────────────────────────────────────────────────────────

pub fn stream_client_started(span: &Span) {
    tracing::debug!(target: CLIENT, parent: span, "rpc streaming client call started");
}

pub fn stream_client_abandoned(span: &Span) {
    tracing::debug!(target: CLIENT, parent: span, outcome = "abandoned", "rpc streaming client call abandoned by its caller");
}

pub fn stream_client_failed(span: &Span, error: &dyn Debug) {
    tracing::warn!(target: CLIENT, parent: span, outcome = "transport_error", transport_error = ?error, "rpc streaming client call failed");
}

pub fn stream_client_progress_decode_failed(span: &Span, error: &dyn Debug) {
    tracing::warn!(target: CLIENT, parent: span, outcome = "transport_error", transport_error = ?error, "rpc streaming client progress decode failed");
}

pub fn stream_client_progress(span: &Span) {
    tracing::trace!(target: CLIENT, parent: span, outcome = "progress", "rpc streaming client progress");
}

pub fn stream_client_success(span: &Span) {
    tracing::debug!(target: CLIENT, parent: span, outcome = "success", "rpc streaming client call finished");
}

pub fn stream_client_success_decode_failed(span: &Span, error: &dyn Debug) {
    tracing::warn!(target: CLIENT, parent: span, outcome = "transport_error", transport_error = ?error, "rpc streaming client success decode failed");
}

pub fn stream_client_app_error(span: &Span) {
    tracing::debug!(target: CLIENT, parent: span, outcome = "app_error", "rpc streaming client call finished");
}

pub fn stream_client_error_decode_failed(span: &Span, error: &dyn Debug) {
    tracing::warn!(target: CLIENT, parent: span, outcome = "transport_error", transport_error = ?error, "rpc streaming client error decode failed");
}

pub fn stream_client_interrupted(span: &Span) {
    tracing::warn!(target: CLIENT, parent: span, outcome = "transport_interrupted", "rpc streaming client call interrupted");
}
