use serde::{Serialize, de::DeserializeOwned};
use crate::{Error, Result, UnaryCodec, Payload};

pub struct JsonCodec;
impl UnaryCodec for JsonCodec {
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes).map_err(|e| Error::Deserialize(e.to_string()))
    }

    fn encode<T: Serialize>(value: &T) -> Result<Payload> {
        let bytes_vec = serde_json::to_vec(value).map_err(|e| Error::Serialize(e.to_string()))?;
        Ok(Payload::new(&bytes_vec))
    }
}