//! Shared **contract types** for stored message templates — plus one pure render helper.
//!
//! This crate is *not* a service and *not* a store. It holds the request/response DTOs and the
//! [`TemplateChannel`] enum that a notification/identity service exchanges over RPC, and a single
//! pure function — [`render_stored_template`] — that renders a stored template body against JSON
//! data with `tera`, **HTML auto-escaping off** (these are SMS/email/WhatsApp bodies, not web
//! pages). *Where* templates are stored is the consumer's concern (e.g. `allex-identity-postgres`
//! keeps a `message_templates` table). Keeping this a leaf crate of pure types lets any service
//! depend on it without pulling in storage.
//!
//! Note: [`TemplateChannel::from_str`] returns `Option` (not `Result`) and is deliberately *not*
//! the `FromStr` trait.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TemplateChannel {
    Email,
    Sms,
    Whatsapp,
}

impl TemplateChannel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Email => "email",
            Self::Sms => "sms",
            Self::Whatsapp => "whatsapp",
        }
    }

    // Returns `Option<Self>` (not `Result`) so it deliberately doesn't match the `FromStr` trait.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "email" => Some(Self::Email),
            "sms" => Some(Self::Sms),
            "whatsapp" => Some(Self::Whatsapp),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertMessageTemplateRequest {
    pub template_key: String,
    pub channel: TemplateChannel,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderMessageTemplateRequest {
    pub template_key: String,
    pub channel: TemplateChannel,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderedMessageResponse {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForgotPasswordMessageRequest {
    pub handle: String,
    pub channel: TemplateChannel,
    pub reset_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForgotPasswordMessageResponse {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TemplateError {
    NotFound,
    Render(String),
    Storage(String),
}

pub fn render_stored_template(
    template_body: &str,
    data: &serde_json::Value,
) -> Result<String, TemplateError> {
    let context =
        tera::Context::from_serialize(data).map_err(|e| TemplateError::Render(e.to_string()))?;
    tera::Tera::one_off(template_body, &context, false)
        .map_err(|e| TemplateError::Render(e.to_string()))
}
