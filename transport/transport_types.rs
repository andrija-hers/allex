use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Opaque serialized payload exchanged across transports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload(Vec<u8>);

impl Payload {
    /// Wraps raw bytes as a transport payload.
    pub fn new(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }

    /// Returns the payload size in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
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
        &self.1.get_method()
    }
    /// Returns the encoded request payload.
    pub fn get_payload(&self) -> &Payload {
        &self.1.get_payload()
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
        &self.1.get_module()
    }
    /// Returns the targeted method name.
    pub fn get_method(&self) -> &str {
        &self.1.get_method()
    }
    /// Returns the encoded request payload.
    pub fn get_payload(&self) -> &Payload {
        &self.1.get_payload()
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
