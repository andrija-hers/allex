//! Generic [Mailgun](https://www.mailgun.com/) HTTP API client.
//!
//! This crate knows nothing about any particular application — it only sends
//! email through Mailgun's `messages` endpoint. A sibling crate (e.g.
//! `allex-aws-mailer`) can provide the same `send`-shaped surface for SES so
//! callers can swap providers without changing call sites.
//!
//! ```no_run
//! # async fn demo() -> Result<(), allex_mailgunner::MailgunError> {
//! use allex_mailgunner::{Mailgun, Email, Region};
//!
//! let mailgun = Mailgun::with_region("key-...", "mg.example.com", Region::Eu);
//! let email = Email::new("ISystem <no-reply@mg.example.com>", "user@example.com")
//!     .subject("Hello")
//!     .text("Plain-text body");
//! let sent = mailgun.send(&email).await?;
//! println!("queued as {}", sent.id);
//! # Ok(())
//! # }
//! ```

use serde::Deserialize;

/// Mailgun serves two regions on distinct hostnames. Pick the one your domain
/// is provisioned in — sending to the wrong region returns 401.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Region {
    /// `https://api.mailgun.net` (default).
    Us,
    /// `https://api.eu.mailgun.net`.
    Eu,
}

impl Region {
    /// Base URL (no trailing slash) for this region.
    pub fn base_url(self) -> &'static str {
        match self {
            Region::Us => "https://api.mailgun.net",
            Region::Eu => "https://api.eu.mailgun.net",
        }
    }

    /// Parse `"us"`/`"eu"` (case-insensitive). Returns `None` for anything else.
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_lowercase().as_str() {
            "us" | "usa" | "" => Some(Region::Us),
            "eu" | "europe" => Some(Region::Eu),
            _ => None,
        }
    }
}

/// A single outbound message. Build with [`Email::new`] and the chainable
/// setters; at least one of [`Email::text`] / [`Email::html`] should be set.
#[derive(Debug, Clone)]
pub struct Email {
    /// `From` header, e.g. `"ISystem <no-reply@mg.example.com>"`.
    pub from: String,
    /// One or more recipient addresses.
    pub to: Vec<String>,
    /// Subject line.
    pub subject: String,
    /// Plain-text body.
    pub text: Option<String>,
    /// HTML body.
    pub html: Option<String>,
}

impl Email {
    /// Start a message with a single recipient.
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: vec![to.into()],
            subject: String::new(),
            text: None,
            html: None,
        }
    }

    /// Add another recipient.
    pub fn add_to(mut self, to: impl Into<String>) -> Self {
        self.to.push(to.into());
        self
    }

    /// Set the subject line.
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = subject.into();
        self
    }

    /// Set the plain-text body.
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    /// Set the HTML body.
    pub fn html(mut self, html: impl Into<String>) -> Self {
        self.html = Some(html.into());
        self
    }
}

/// Mailgun's JSON response to a successful send.
#[derive(Debug, Clone, Deserialize)]
pub struct SendResponse {
    /// Queued message id, e.g. `"<2025...@mg.example.com>"`.
    pub id: String,
    /// Human-readable status, typically `"Queued. Thank you."`.
    pub message: String,
}

/// Anything that can go wrong while sending.
#[derive(Debug)]
pub enum MailgunError {
    /// The HTTP request itself failed (DNS, TLS, connection, decode).
    Http(reqwest::Error),
    /// Mailgun returned a non-2xx status. Carries the status code and raw body
    /// (which usually explains the problem — bad domain, unauthorized recipient
    /// on a sandbox domain, invalid key, wrong region, etc.).
    Api { status: u16, body: String },
}

impl std::fmt::Display for MailgunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MailgunError::Http(e) => write!(f, "mailgun request failed: {e}"),
            MailgunError::Api { status, body } => {
                write!(f, "mailgun returned {status}: {body}")
            }
        }
    }
}

impl std::error::Error for MailgunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MailgunError::Http(e) => Some(e),
            MailgunError::Api { .. } => None,
        }
    }
}

/// A configured Mailgun client. Cheap to clone (wraps a `reqwest::Client`,
/// which is itself an `Arc` internally).
#[derive(Debug, Clone)]
pub struct Mailgun {
    client: reqwest::Client,
    api_key: String,
    domain: String,
    base_url: String,
}

impl Mailgun {
    /// Client for the US region (`api.mailgun.net`).
    pub fn new(api_key: impl Into<String>, domain: impl Into<String>) -> Self {
        Self::with_region(api_key, domain, Region::Us)
    }

    /// Client for a specific [`Region`].
    pub fn with_region(
        api_key: impl Into<String>,
        domain: impl Into<String>,
        region: Region,
    ) -> Self {
        Self::with_base_url(api_key, domain, region.base_url())
    }

    /// Client pointed at an explicit base URL (no trailing slash). Useful for
    /// tests against a mock server, or future/self-hosted endpoints.
    pub fn with_base_url(
        api_key: impl Into<String>,
        domain: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            domain: domain.into(),
            base_url: base_url.into(),
        }
    }

    /// The domain this client sends through.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Send a message. Returns Mailgun's queue id on success.
    pub async fn send(&self, email: &Email) -> Result<SendResponse, MailgunError> {
        let url = format!("{}/v3/{}/messages", self.base_url, self.domain);

        let mut form: Vec<(&str, &str)> = vec![("from", email.from.as_str())];
        for recipient in &email.to {
            form.push(("to", recipient.as_str()));
        }
        form.push(("subject", email.subject.as_str()));
        if let Some(text) = &email.text {
            form.push(("text", text.as_str()));
        }
        if let Some(html) = &email.html {
            form.push(("html", html.as_str()));
        }

        let response = self
            .client
            .post(&url)
            .basic_auth("api", Some(&self.api_key))
            .form(&form)
            .send()
            .await
            .map_err(MailgunError::Http)?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(MailgunError::Api {
                status: status.as_u16(),
                body,
            });
        }

        response.json::<SendResponse>().await.map_err(MailgunError::Http)
    }
}
