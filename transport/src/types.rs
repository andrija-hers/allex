use serde::{Serialize, Deserialize, de::DeserializeOwned};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload(Vec<u8>);

impl Payload {
    pub fn new(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn get_bytes_forever(self) -> Vec<u8> {
        self.0
    }

    pub fn decode<C, T>(&self) -> Result<T>
    where
        C: UnaryCodec,
        T: DeserializeOwned,
    {
        C::decode::<T>(&self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Error {
    Serialize(String),
    Deserialize(String),
    SendRequest(String),
    ModuleNotFound,
    ServerNotFound,
    ReceiveResponse(String),
    App(Payload)
}

pub type Result<T> = core::result::Result<T, Error>;

pub trait UnaryCodec {
    /// Decode bytes into type T
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T>;

    /// Encode value of type T into bytes
    fn encode<T: Serialize>(value: &T) -> Result<Payload>;
}

pub trait Encodable {
    fn encode <C: UnaryCodec> (&self) -> Result<Payload>;
}

impl <T: Serialize> Encodable for T {
    fn encode <C: UnaryCodec> (&self) -> Result<Payload> {
        C::encode(&self)
    }
}

impl Error {
    pub fn surely_encode<C: UnaryCodec> (&self) -> Payload {
        match self.encode::<C>() {
            Ok(encoded) => encoded,
            Err(_) => Payload::new("Error in encoding Error".as_bytes())
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MethodCall (String, Payload);

impl MethodCall {
    pub fn new<C: UnaryCodec, P: Serialize> (method: &str, payload: P) -> Result<Self> {
        Ok(MethodCall (method.to_string(), C::encode::<P>(&payload)? ))
    }
    pub fn get_method(&self) -> &str {
        &self.0
    }
    pub fn get_payload(&self) -> &Payload {
        &self.1
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleMethodCall (String, MethodCall);

impl ModuleMethodCall {
    pub fn new<C: UnaryCodec, P: Serialize> (module: &str, method: &str, payload: P) -> Result<Self> {
        Ok(ModuleMethodCall(module.to_string(), MethodCall::new::<C, P>(method, payload)?))
    }
    pub fn get_module(&self) -> &str {
        &self.0
    }
    pub fn get_method(&self) -> &str {
        &self.1.get_method()
    }
    pub fn get_payload(&self) -> &Payload {
        &self.1.get_payload()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayedModuleMethodCall (String, ModuleMethodCall);

impl RelayedModuleMethodCall {
    pub fn new<C: UnaryCodec, P: Serialize> (server: &str, module: &str, method: &str, payload: P) -> Result<Self> {
        Ok(RelayedModuleMethodCall(server.to_string(), ModuleMethodCall::new::<C, P>(module, method, payload)?))
    }
    pub fn get_server(&self) -> &str {
        &self.0
    }
    pub fn get_module(&self) -> &str {
        &self.1.get_module()
    }
    pub fn get_method(&self) -> &str {
        &self.1.get_method()
    }
    pub fn get_payload(&self) -> &Payload {
        &self.1.get_payload()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CallResponse {
    Success(Payload),
    Error(Payload),
    Progress(Payload)
}



