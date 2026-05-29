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
