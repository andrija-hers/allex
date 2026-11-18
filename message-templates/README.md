# allex-message-templates

> Stored message templates with Tera rendering.

Part of **[allex](https://github.com/andrija-hers/allex)** — a contract-first RPC framework for Rust. See the repository for the workspace overview and `AGENTS.md`, the project's orientation guide.

## The problem it solves

Several allex services (identity, notifications) need to exchange the *same*
template DTOs over RPC — "upsert this template", "render this template with this
data" — and to turn a stored template body into a finished SMS / email / WhatsApp
message. But **where** templates live differs per service: `allex-identity-postgres`
keeps a `message_templates` table; another service might store them somewhere else
entirely.

So this crate is deliberately **not a service and not a store**. It is a *leaf of
pure contract types* (`TemplateChannel`, `UpsertMessageTemplateRequest`,
`RenderMessageTemplateRequest`, `RenderedMessageResponse`, …) plus one pure
function, `render_stored_template`. Keeping it a types-only leaf lets any service
depend on it without dragging in a storage engine.

One deliberate choice: rendering runs [Tera](https://crates.io/crates/tera) with
**HTML auto-escaping OFF**. These are message bodies (SMS/email/WhatsApp), not web
pages — a `{{ code }}` must come through literally, not HTML-encoded.

## Example

```rust
use allex_message_templates::render_stored_template;
use serde_json::json;

// `body` is whatever you loaded from your own storage:
let body = "Hi {{ name }}, your verification code is {{ code }}.";

let message = render_stored_template(
    body,
    &json!({ "name": "Ada", "code": "123456" }),
)?;

assert_eq!(message, "Hi Ada, your verification code is 123456.");
```

Storing and retrieving that `body` is the caller's job; this crate only models the
DTOs and renders. Pair it with `allex-mailgunner` (email) or any other sink.

## Docs & license

API documentation: **[docs.rs/allex-message-templates](https://docs.rs/allex-message-templates)** (design notes live in the crate's `//!` module docs). Licensed under **MIT OR Apache-2.0**.
