use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Whether human-readable formats write the **legacy** payload form: one JSON
/// number per byte, instead of a string.
///
/// This exists for one situation, and it is worth naming precisely: a project
/// whose clients are **installed** rather than served — an app behind store
/// review, a sideloaded binary in the field. Readers accept both forms, so the
/// break is only ever in what a peer *writes*, and both directions break at once
/// (an old client cannot read a new server's reply; a new client's request is
/// rejected by an old server). There is no deploy ordering that avoids the gap —
/// but there is a switch: deploy the server writing the legacy form, ship the
/// client that reads both, then flip this off and delete it a release later.
///
/// Seeded once from `ALLEX_LEGACY_PAYLOAD_WRITES` (`1` or `true`), so a
/// deployment can turn it on without a code change, and overridable at runtime
/// via [`set_legacy_payload_writes`] for tests and for hosts that would rather
/// configure it themselves.
///
/// **Default off.** The string form is 3.2–3.4× faster to encode and 3.1× smaller
/// uncompressed; a consumer with no installed clients — anything served by the
/// same host as its API — should never pay for a compatibility window it does not
/// have.
static LEGACY_PAYLOAD_WRITES: OnceLock<AtomicBool> = OnceLock::new();

fn legacy_writes_cell() -> &'static AtomicBool {
    LEGACY_PAYLOAD_WRITES.get_or_init(|| {
        AtomicBool::new(matches!(
            std::env::var("ALLEX_LEGACY_PAYLOAD_WRITES").as_deref(),
            Ok("1") | Ok("true")
        ))
    })
}

/// Whether payloads are currently written in the legacy array-of-bytes form.
pub fn legacy_payload_writes() -> bool {
    legacy_writes_cell().load(Ordering::Relaxed)
}

/// Turns the legacy payload form on or off for this process, overriding the
/// environment. Takes effect on the next value serialized.
pub fn set_legacy_payload_writes(on: bool) {
    legacy_writes_cell().store(on, Ordering::Relaxed);
}

/// Opaque serialized payload exchanged across transports.
///
/// The wire form depends on the enclosing format. Binary formats take the bytes
/// as they are. Human-readable ones (JSON) take them as a **string** when they
/// are valid UTF-8, because the derived `Vec<u8>` encoding writes one JSON
/// number per byte: a 2.9 MB payload went out as 10.1 MB of digits and commas
/// — 3 million array elements the receiver then had to walk before it could
/// even begin parsing what they spelled. Payloads are almost always UTF-8 JSON
/// produced by a codec, so a string costs only the escaping.
#[derive(Debug, Clone)]
pub struct Payload(Vec<u8>);

impl Serialize for Payload {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match (serializer.is_human_readable(), std::str::from_utf8(&self.0)) {
            // `legacy_payload_writes()` forces the array form even here, for a
            // deployment still serving clients that predate the string encoding.
            (true, Ok(text)) if !legacy_payload_writes() => serializer.serialize_str(text),
            // Binary formats, and the rare non-UTF-8 payload, keep the byte
            // sequence — for JSON that is the old array form, which
            // `Deserialize` still accepts.
            _ => serializer.collect_seq(self.0.iter()),
        }
    }
}

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct PayloadVisitor;

        impl<'de> serde::de::Visitor<'de> for PayloadVisitor {
            type Value = Payload;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a payload as a string, byte array, or sequence of bytes")
            }

            fn visit_str<E: serde::de::Error>(
                self,
                value: &str,
            ) -> std::result::Result<Payload, E> {
                Ok(Payload(value.as_bytes().to_vec()))
            }

            fn visit_bytes<E: serde::de::Error>(
                self,
                value: &[u8],
            ) -> std::result::Result<Payload, E> {
                Ok(Payload(value.to_vec()))
            }

            fn visit_byte_buf<E: serde::de::Error>(
                self,
                value: Vec<u8>,
            ) -> std::result::Result<Payload, E> {
                Ok(Payload(value))
            }

            /// The legacy array-of-numbers form, still emitted by binary
            /// formats and by peers that predate the string encoding.
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Payload, A::Error> {
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(Payload(bytes))
            }
        }

        deserializer.deserialize_any(PayloadVisitor)
    }
}

impl Payload {
    /// Wraps raw bytes as a transport payload.
    pub fn new(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }

    /// Returns the payload size in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the payload has no bytes.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Borrows the underlying serialized bytes.
    pub fn get_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the payload and returns its owned bytes.
    pub fn get_bytes_forever(self) -> Vec<u8> {
        self.0
    }

    /// Decodes the payload with the chosen codec into `T`.
    pub fn decode<C, T>(&self) -> Result<T>
    where
        C: UnaryCodec,
        T: DeserializeOwned,
    {
        C::decode::<T>(&self.0)
    }
}

/// Transport-level errors shared by all transports in the workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Error {
    /// Encoding a value into bytes failed.
    Serialize(String),
    /// Decoding bytes into a value failed.
    Deserialize(String),
    /// Sending a request to the peer failed.
    SendRequest(String),
    /// The addressed module was not registered.
    ModuleNotFound,
    /// The addressed upstream server was not registered.
    ServerNotFound,
    /// The caller is not authorized to invoke the requested method.
    AccessForbidden,
    /// Receiving or interpreting the peer response failed.
    ReceiveResponse(String),
    /// The peer returned an application-level error payload.
    App(Payload),
}

/// Convenience alias for results returned by transport APIs.
pub type Result<T> = core::result::Result<T, Error>;

/// Codec used to turn typed values into [`Payload`]s and back.
pub trait UnaryCodec {
    /// Decode bytes into type T
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T>;

    /// Encode value of type T into bytes
    fn encode<T: Serialize>(value: &T) -> Result<Payload>;

    /// MIME type to use in the HTTP Content-Type header for this codec.
    fn content_type() -> &'static str
    where
        Self: Sized,
    {
        "application/json"
    }
}

/// Helper trait for values that can be encoded with a [`UnaryCodec`].
pub trait Encodable {
    /// Encodes `self` into a [`Payload`] using the selected codec.
    fn encode<C: UnaryCodec>(&self) -> Result<Payload>;
}

impl<T: Serialize> Encodable for T {
    fn encode<C: UnaryCodec>(&self) -> Result<Payload> {
        C::encode(&self)
    }
}

impl Error {
    /// Encodes the error as an application payload, falling back to a plain message on failure.
    pub fn surely_encode<C: UnaryCodec>(&self) -> Payload {
        match self.encode::<C>() {
            Ok(encoded) => encoded,
            Err(_) => Payload::new("Error in encoding Error".as_bytes()),
        }
    }
}

/// One-shot RPC call addressed only by method name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MethodCall(String, Payload);

impl MethodCall {
    /// Creates a new method call by encoding the supplied request payload.
    pub fn new<C: UnaryCodec, P: Serialize>(method: &str, payload: P) -> Result<Self> {
        Ok(MethodCall(method.to_string(), C::encode::<P>(&payload)?))
    }
    /// Returns the targeted method name.
    pub fn get_method(&self) -> &str {
        &self.0
    }
    /// Returns the encoded request payload.
    pub fn get_payload(&self) -> &Payload {
        &self.1
    }
}

/// One-shot RPC call addressed by module and method name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleMethodCall(String, MethodCall);

impl ModuleMethodCall {
    /// Creates a new module-scoped method call by encoding the supplied request payload.
    pub fn new<C: UnaryCodec, P: Serialize>(
        module: &str,
        method: &str,
        payload: P,
    ) -> Result<Self> {
        Ok(ModuleMethodCall(
            module.to_string(),
            MethodCall::new::<C, P>(method, payload)?,
        ))
    }
    /// Returns the targeted module name.
    pub fn get_module(&self) -> &str {
        &self.0
    }
    /// Returns the targeted method name.
    pub fn get_method(&self) -> &str {
        self.1.get_method()
    }
    /// Returns the encoded request payload.
    pub fn get_payload(&self) -> &Payload {
        self.1.get_payload()
    }
}

/// One-shot RPC call relayed through a named upstream server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayedModuleMethodCall(String, ModuleMethodCall);

impl RelayedModuleMethodCall {
    /// Creates a new relayed module call by encoding the supplied request payload.
    pub fn new<C: UnaryCodec, P: Serialize>(
        server: &str,
        module: &str,
        method: &str,
        payload: P,
    ) -> Result<Self> {
        Ok(RelayedModuleMethodCall(
            server.to_string(),
            ModuleMethodCall::new::<C, P>(module, method, payload)?,
        ))
    }
    /// Returns the targeted server name.
    pub fn get_server(&self) -> &str {
        &self.0
    }
    /// Returns the targeted module name.
    pub fn get_module(&self) -> &str {
        self.1.get_module()
    }
    /// Returns the targeted method name.
    pub fn get_method(&self) -> &str {
        self.1.get_method()
    }
    /// Returns the encoded request payload.
    pub fn get_payload(&self) -> &Payload {
        self.1.get_payload()
    }
}

/// Streaming or terminal response frame returned by chunked transports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CallResponse {
    /// Terminal success payload.
    Success(Payload),
    /// Terminal application-level error payload.
    Error(Payload),
    /// Non-terminal progress payload.
    Progress(Payload),
}

/// Uninhabited success type for streaming RPC methods that never terminate
/// normally — i.e. "billboard" streams that run until the connection drops.
///
/// Use `Never` as the `T` parameter in
/// `ProgressableFuture<P, Never, E>` / `Stream<AllexStep<P, Never, E>>`
/// to document and enforce that no success frame is ever emitted.
///
/// The `Serialize` and `Deserialize` impls are formally required by the
/// `#[rpc_module]` macro's wire-protocol bounds, but they are unreachable
/// in practice because a `Never` value cannot be constructed.
#[derive(Debug, Clone)]
pub enum Never {}

impl serde::Serialize for Never {
    fn serialize<S: serde::Serializer>(&self, _: S) -> core::result::Result<S::Ok, S::Error> {
        match *self {}
    }
}

impl<'de> serde::Deserialize<'de> for Never {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> core::result::Result<Self, D::Error> {
        // Formally unreachable — the server never sends a success frame for
        // a billboard stream.  Return a descriptive error just in case.
        let _ = d;
        Err(serde::de::Error::custom(
            "received unexpected success frame on a Never stream",
        ))
    }
}

impl From<std::convert::Infallible> for Never {
    fn from(x: std::convert::Infallible) -> Self {
        match x {}
    }
}

#[cfg(test)]
mod payload_wire_tests {
    use super::*;

    /// The legacy-write flag is process-global, so every test that toggles it —
    /// or that asserts the form it selects — takes this lock first. Cargo runs
    /// tests in threads within one binary, so without it the switch test below
    /// would flip the wire form underneath a concurrently running one.
    static FLAG_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn lock_flag() -> std::sync::MutexGuard<'static, ()> {
        FLAG_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn json_carries_a_payload_as_a_string_not_a_byte_array() {
        let _guard = lock_flag();
        let payload = Payload::new(br#"{"frame":"initial"}"#);
        let json = serde_json::to_string(&payload).expect("serialize");
        assert_eq!(json, r#""{\"frame\":\"initial\"}""#);
    }

    /// The staged-rollout path: a server can be told to keep writing the form
    /// that clients already in the field know how to read.
    #[test]
    fn the_legacy_write_switch_puts_the_array_form_back_on_the_wire() {
        let _guard = lock_flag();
        let payload = Payload::new(br#"{"a":1}"#);

        // Set explicitly rather than asserting the default: the default is
        // seeded from the environment, and a test must not fail because the
        // machine running it happens to export the variable.
        set_legacy_payload_writes(false);
        assert_eq!(
            serde_json::to_string(&payload).expect("serialize"),
            r#""{\"a\":1}""#
        );

        set_legacy_payload_writes(true);
        let json = serde_json::to_string(&payload).expect("serialize");
        assert_eq!(json, "[123,34,97,34,58,49,125]");

        // Still readable by a peer on either form — which is the whole point:
        // the switch stages the rollout instead of introducing a second break.
        let back: Payload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.get_bytes(), payload.get_bytes());

        set_legacy_payload_writes(false);
        assert_eq!(
            serde_json::to_string(&payload).expect("serialize"),
            r#""{\"a\":1}""#
        );
    }

    /// The size claim, on a payload the size of a real snapshot rather than a
    /// toy: the array form spends ~4 characters per byte, the string form one
    /// (plus an escape per quote).
    #[test]
    fn the_string_form_is_a_fraction_of_the_array_form() {
        let _guard = lock_flag();
        let row = r#"{"schedule_id":"example::DAILY_DIVIDENDS","status":"Processed"},"#;
        let payload = Payload::new(row.repeat(2000).as_bytes());
        let string_form = serde_json::to_string(&payload).expect("serialize").len();
        let array_form = serde_json::to_string(&payload.get_bytes().to_vec())
            .expect("array")
            .len();
        assert!(
            string_form * 3 < array_form,
            "string {string_form} vs array {array_form}"
        );
    }

    #[test]
    fn a_payload_round_trips_through_json() {
        let payload = Payload::new(br#"{"a":[1,2,3],"b":"x"}"#);
        let json = serde_json::to_string(&payload).expect("serialize");
        let back: Payload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.get_bytes(), payload.get_bytes());
    }

    /// Peers that predate the string form still send arrays of numbers; keep
    /// reading them, or an old client cannot talk to a new server.
    #[test]
    fn the_legacy_byte_array_form_is_still_accepted() {
        let legacy = "[123,34,97,34,58,49,125]"; // {"a":1}
        let payload: Payload = serde_json::from_str(legacy).expect("legacy form");
        assert_eq!(payload.get_bytes(), br#"{"a":1}"#);
    }

    /// Non-UTF-8 bytes cannot ride as a string; they fall back to the array
    /// form rather than failing or corrupting.
    #[test]
    fn non_utf8_payloads_fall_back_to_the_array_form() {
        let payload = Payload::new(&[0xff, 0xfe, 0x00]);
        let json = serde_json::to_string(&payload).expect("serialize");
        assert_eq!(json, "[255,254,0]");
        let back: Payload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.get_bytes(), &[0xff, 0xfe, 0x00]);
    }
}
