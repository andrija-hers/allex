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
