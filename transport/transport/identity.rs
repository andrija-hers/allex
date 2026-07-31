use crate::{Error, Payload, Result};

use super::{
    ServerSideChunkedTransportItem, ServerSideOneShotTransport, ServerSideOneShotTransportItem,
    ServerSideSerialTransport, ServerSideSerialTransportItem,
};

/// A transport item that carries the authenticated caller's role, in addition
/// to the base [`ServerSideOneShotTransportItem`] + [`ServerSideChunkedTransportItem`]
/// contracts.
pub trait IdentityTransportItem:
    ServerSideOneShotTransportItem + ServerSideChunkedTransportItem
{
    /// Returns the authenticated caller role, if the transport established one.
    fn caller_role(&self) -> Option<&str>;

    /// Returns the caller's **self-reported** client identifier, if it sent one —
    /// by convention `platform/version+build`, e.g. `ios/1.0.2+42`.
    ///
    /// Unlike [`caller_role`](Self::caller_role) this is not validated by
    /// anything: the client picks the string. It exists so a host can record
    /// *which builds are actually in the field*, which is the difference between
    /// retiring a compatibility shim on evidence and retiring it on a guess. It
    /// must never reach an authorization decision.
    ///
    /// Defaults to `None`, so a transport that does not carry one needs no change.
    fn client_info(&self) -> Option<&str> {
        None
    }

    /// Completes the request with an application-level error payload.
    fn respond_with_app_error(
        self,
        payload: Payload,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Completes the request with a transport-level error.
    fn respond_with_transport_error(
        self,
        error: Error,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// Marker supertrait: a [`ServerSideOneShotTransport`] whose items are
/// guaranteed to carry the authenticated caller's role.
///
/// Implementing this trait on a transport communicates that all items it
/// produces implement [`IdentityTransportItem`], so generic server dispatch
/// code can bound on `T: IdentityOneShotTransport` and call
/// `item.caller_role()` without knowing the concrete transport type.
pub trait IdentityOneShotTransport: ServerSideOneShotTransport
where
    Self::Item: IdentityTransportItem,
{
}

/// A Serial transport item that carries the authenticated caller's role.
pub trait IdentitySerialTransportItem: ServerSideSerialTransportItem {
    /// Returns the authenticated caller role, if the transport established one.
    fn caller_role(&self) -> Option<&str>;
}

/// Marker supertrait: a [`ServerSideSerialTransport`] whose items carry the
/// authenticated caller's role.
pub trait IdentitySerialTransport: ServerSideSerialTransport
where
    Self::Item: IdentitySerialTransportItem,
{
}
