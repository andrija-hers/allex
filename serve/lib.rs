//! Generic server-side dispatch for allex one-shot transports.
//!
//! Every allex backend used to hand-write the same loop that drains the
//! transport's incoming stream, decodes the call, routes streaming-vs-unary, and
//! maps responses. Those copies all shared one flaw: they `await`ed each request
//! inline, serializing the entire server behind a single in-flight handler.
//!
//! [`serve_dispatch`] is that loop, written once and correctly: it spawns a task
//! per request so concurrency is bounded by the connection pool, not by one
//! query at a time. It also installs a standard [`CallerContext`] task-local so
//! module implementations can read the authenticated caller via
//! [`caller_handle`] / [`caller_role`] without each project defining its own.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use execution::{ModuleExecutor, Registry};
use futures::StreamExt;
use transport::{
    Error as TransportError, IdentityTransportItem, ModuleMethodCall,
    ServerSideChunkedTransportItem, ServerSideOneShotTransport, ServerSideOneShotTransportItem,
    UnaryCodec,
};

tokio::task_local! {
    static CALLER: CallerContext;
    // Set by `deny_access()` inside a method body to escalate a per-object
    // authorization failure to a transport-level 403 (see `deny_access`).
    static ACCESS_DENIED: Arc<AtomicBool>;
}

/// The authenticated caller for the current request.
#[derive(Clone, Default, Debug)]
pub struct CallerContext {
    /// The caller's handle (username), if authenticated.
    pub handle: Option<String>,
    /// The caller's role, if any.
    pub role: Option<String>,
    /// An optional opaque, project-defined profile payload: the third
    /// `\x1e`-separated segment of the bearer-validated caller string (e.g. a
    /// JSON blob of per-user rights flags a store baked into the token and
    /// re-emitted from `validate_bearer`). `None` for the classic two-segment
    /// `handle\x1erole` encoding, so this is fully backward compatible. Decode
    /// it into your own type via [`caller_profile`]; allex treats it as bytes.
    pub profile: Option<String>,
    /// What the caller says it is — `platform/version+build`, e.g.
    /// `ios/1.0.2+42` — from the `x-allex-client` header, when it sent one.
    ///
    /// Unlike every other field here this does **not** come from the validated
    /// bearer token: the client picks the string, so it is telemetry and never a
    /// credential. It is here because "which builds are still in the field" is
    /// otherwise unanswerable, and that question gates retiring any
    /// compatibility shim — see the payload legacy-write switch in
    /// `allex-transport`.
    pub client: Option<String>,
}

impl CallerContext {
    /// Parses the `handle\x1erole[\x1eprofile]` string allex identity stores
    /// encode into the bearer-validated caller field (see
    /// `PostgresIdentityStore::validate_bearer`). The optional third segment is
    /// an opaque, project-defined profile (see [`CallerContext::profile`]).
    pub fn parse(encoded: Option<&str>) -> Self {
        match encoded {
            None => Self::default(),
            Some(s) => {
                // `splitn(3)` so the opaque profile segment may itself contain
                // `\x1e` (e.g. arbitrary JSON) without being truncated, while
                // handle/role extraction is unchanged from the two-segment form.
                let mut parts = s.splitn(3, '\u{1e}');
                let handle = parts.next().filter(|p| !p.is_empty()).map(str::to_owned);
                let role = parts
                    .next()
                    .map(str::to_owned)
                    .or_else(|| Some(s.to_owned()));
                let profile = parts.next().filter(|p| !p.is_empty()).map(str::to_owned);
                Self {
                    handle,
                    role,
                    profile,
                    client: None,
                }
            }
        }
    }
}

/// Returns the authenticated caller for the current request (default if unset).
pub fn caller() -> CallerContext {
    CALLER.try_with(|c| c.clone()).unwrap_or_default()
}

/// Returns the authenticated caller's handle for the current request, if any.
pub fn caller_handle() -> Option<String> {
    CALLER.try_with(|c| c.handle.clone()).ok().flatten()
}

/// Returns the authenticated caller's role for the current request, if any.
pub fn caller_role() -> Option<String> {
    CALLER.try_with(|c| c.role.clone()).ok().flatten()
}

/// Returns the authenticated caller's opaque profile segment for the current
/// request, if the store attached one (see [`CallerContext::profile`]).
///
/// This is the project-defined third segment of the bearer-validated caller
/// string; allex carries it verbatim. Decode it into your own type, e.g.
/// `caller_profile().and_then(|p| serde_json::from_str::<MyRights>(&p).ok())`.
pub fn caller_profile() -> Option<String> {
    CALLER.try_with(|c| c.profile.clone()).ok().flatten()
}

/// Returns what the caller said it is for the current request, if it sent an
/// `x-allex-client` header — `platform/version+build` by convention.
///
/// Self-reported. Record it, chart it, decide with it; never authorize on it.
pub fn caller_client() -> Option<String> {
    CALLER.try_with(|c| c.client.clone()).ok().flatten()
}

/// Marks the current RPC call as **access-denied**, so the server responds with a
/// transport-level Forbidden (**HTTP 403**) instead of a 422 application error —
/// whatever domain error your method returns afterwards.
///
/// Use it for **per-object authorization** inside a method body — "only the creator
/// may edit this", "you can't read someone else's record" — the fine-grained check
/// the coarse `#[allowed_roles(..)]` gate can't express. The `#[allowed_roles]` gate
/// already yields 403 for a role mismatch; this extends the same status to method-body
/// decisions, so per-object denials stop masquerading as 422 domain errors.
///
/// Outside a served request (a direct call or a unit test) it is a **no-op**, so your
/// method's returned error is used as-is. Prefer [`authorize`] for the common
/// check-and-return shape. Applies to **unary** methods (streaming methods signal
/// denial with a terminal error frame).
pub fn deny_access() {
    let _ = ACCESS_DENIED.try_with(|flag| flag.store(true, Ordering::Relaxed));
}

/// Per-object authorization guard: returns `Ok(())` when `authorized`, otherwise calls
/// [`deny_access`] (→ HTTP 403) and returns `Err(forbidden_error)` so you can `?` it.
///
/// ```ignore
/// async fn edit(&self, req: EditReq) -> Result<Edited, MyError> {
///     let doc = self.load(req.id).await?;
///     allex_serve::authorize(doc.owner.as_deref() == caller_handle().as_deref(),
///                            MyError::Forbidden)?;   // 403 if not the owner
///     // … proceed …
/// }
/// ```
///
/// The `forbidden_error` is your own domain error: it becomes the (bodyless) 403 within a
/// served request, and is returned normally when called outside one.
pub fn authorize<E>(authorized: bool, forbidden_error: E) -> Result<(), E> {
    if authorized {
        Ok(())
    } else {
        deny_access();
        Err(forbidden_error)
    }
}

/// Drains `transport`'s incoming requests and dispatches each through `registry`,
/// spawning a task per request so they run concurrently.
///
/// `C` is the wire codec (e.g. `transport::JsonCodec`); pass it via turbofish:
/// `serve_dispatch::<_, JsonCodec>(transport, registry)`. Streaming modules must
/// be registered with [`Registry::register_streaming_module`] (or
/// [`Registry::register`]); unary-only modules with [`Registry::register_module`].
pub async fn serve_dispatch<T, C>(transport: T, registry: Arc<Registry>)
where
    T: ServerSideOneShotTransport + Send + 'static,
    T::ItemStream: Send + Unpin,
    T::Item: ServerSideOneShotTransportItem
        + IdentityTransportItem
        + ServerSideChunkedTransportItem
        + Send
        + 'static,
    C: UnaryCodec + Send + Sync + 'static,
{
    let mut stream = transport.get_incoming_stream().await;
    while let Some(item) = stream.next().await {
        let registry = registry.clone();
        tokio::spawn(handle_request::<T, C>(item, registry));
    }
}

async fn handle_request<T, C>(item: T::Item, registry: Arc<Registry>)
where
    T: ServerSideOneShotTransport,
    T::Item: ServerSideOneShotTransportItem
        + IdentityTransportItem
        + ServerSideChunkedTransportItem
        + Send
        + 'static,
    C: UnaryCodec + Send + Sync + 'static,
{
    let mut ctx = CallerContext::parse(item.caller_role());
    // Not part of the token-encoded caller string, so it is attached here rather
    // than in `parse` — different provenance, different trust.
    ctx.client = item.client_info().map(str::to_owned);
    let role = ctx.role.clone();

    let call = match item.get_payload().decode::<C, ModuleMethodCall>() {
        Ok(call) => call,
        Err(error) => {
            let _ = item.respond_with(error.surely_encode::<C>()).await;
            return;
        }
    };
    let module = call.get_module().to_owned();
    let method = call.get_method().to_owned();
    let payload = call.get_payload().clone();

    // Streaming first. sync_scope sets CALLER while the streaming method builds
    // its future, so the method can capture the caller before that future is
    // polled later by the transport.
    let streamed = CALLER.sync_scope(ctx.clone(), || {
        registry.execute_streaming(&module, &method, payload.clone(), role.as_deref())
    });
    if let Some(stream_resp) = streamed {
        let _ = item.respond_with_stream(stream_resp).await;
        return;
    }

    // Unary. Wrap dispatch in an ACCESS_DENIED scope so a method can escalate a
    // per-object authorization failure to a transport-level 403 via `deny_access()`.
    let access_denied = Arc::new(AtomicBool::new(false));
    let response = ACCESS_DENIED
        .scope(
            access_denied.clone(),
            CALLER.scope(ctx, async {
                registry
                    .execute(&module, &method, payload, role.as_deref())
                    .await
            }),
        )
        .await;

    if access_denied.load(Ordering::Relaxed) {
        // A method called `deny_access()`: respond 403, ignoring the domain error
        // it returned (which would otherwise have been a 422 application error).
        let _ = item
            .respond_with_transport_error(TransportError::AccessForbidden)
            .await;
        return;
    }

    match response {
        Ok(payload) => {
            let _ = item.respond_with(payload).await;
        }
        Err(TransportError::App(inner)) => {
            let _ = item.respond_with_app_error(inner).await;
        }
        Err(error) => {
            let _ = item.respond_with_transport_error(error).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RS: char = '\u{1e}';

    #[test]
    fn parse_none_is_default() {
        let c = CallerContext::parse(None);
        assert_eq!(c.handle, None);
        assert_eq!(c.role, None);
        assert_eq!(c.profile, None);
    }

    #[test]
    fn parse_classic_two_segment_unchanged() {
        // The historical `handle\x1erole` encoding must parse exactly as before,
        // with no profile — this is the epicfix-compatibility guarantee.
        let c = CallerContext::parse(Some(&format!("alice{RS}operator")));
        assert_eq!(c.handle.as_deref(), Some("alice"));
        assert_eq!(c.role.as_deref(), Some("operator"));
        assert_eq!(c.profile, None);
    }

    #[test]
    fn parse_third_segment_becomes_profile() {
        let c = CallerContext::parse(Some(&format!("alice{RS}user{RS}{{\"r_admin\":true}}")));
        assert_eq!(c.handle.as_deref(), Some("alice"));
        assert_eq!(c.role.as_deref(), Some("user"));
        assert_eq!(c.profile.as_deref(), Some("{\"r_admin\":true}"));
    }

    #[test]
    fn parse_profile_may_contain_record_separator() {
        // splitn(3) keeps everything after the second separator in the profile,
        // so an opaque payload containing `\x1e` survives intact.
        let profile = format!("a{RS}b");
        let c = CallerContext::parse(Some(&format!("alice{RS}user{RS}{profile}")));
        assert_eq!(c.role.as_deref(), Some("user"));
        assert_eq!(c.profile.as_deref(), Some(profile.as_str()));
    }

    #[test]
    fn parse_bare_string_keeps_legacy_role_fallback() {
        // A single segment with no separator preserves the pre-existing quirk:
        // handle and role both resolve to the whole string, profile stays None.
        let c = CallerContext::parse(Some("operator"));
        assert_eq!(c.handle.as_deref(), Some("operator"));
        assert_eq!(c.role.as_deref(), Some("operator"));
        assert_eq!(c.profile, None);
    }

    #[test]
    fn authorize_true_does_not_deny() {
        let flag = Arc::new(AtomicBool::new(false));
        ACCESS_DENIED.sync_scope(flag.clone(), || {
            assert!(authorize(true, "unused").is_ok());
        });
        assert!(!flag.load(Ordering::Relaxed));
    }

    #[test]
    fn authorize_false_denies_and_returns_the_error() {
        let flag = Arc::new(AtomicBool::new(false));
        ACCESS_DENIED.sync_scope(flag.clone(), || {
            let result: Result<(), &str> = authorize(false, "forbidden");
            assert_eq!(result, Err("forbidden"));
        });
        assert!(flag.load(Ordering::Relaxed));
    }

    #[test]
    fn deny_outside_a_request_is_a_noop_and_keeps_the_error() {
        // No ACCESS_DENIED scope (e.g. a direct call or unit test): must not panic,
        // and the domain error is still returned for the caller to handle normally.
        deny_access();
        let result: Result<(), &str> = authorize(false, "still-an-error");
        assert_eq!(result, Err("still-an-error"));
    }
}
