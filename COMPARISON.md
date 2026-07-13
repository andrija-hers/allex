# How allex compares

allex overlaps with **gRPC**, **tRPC**, and **ConnectRPC** on one idea:
*contract-first RPC where you don't hand-write the client.* But it occupies a
specific niche the other three don't:

> **A Rust-native, contract-from-code (a Rust trait, no separate IDL) RPC
> *application framework* with first-class, generated Flutter/Dart and browser/TS
> clients — and identity, authorization, and hosting in the box.**

Each of the three peers below beats allex on its own axis; none of them sit at
that intersection. This page is deliberately honest about where allex is behind —
see [Honest gaps](#honest-gaps) and the project's
[Support & contributions](README.md#support--contributions) note (allex is 0.x and
solo-maintained).

## At a glance

| Axis | allex | gRPC | tRPC | ConnectRPC |
|---|---|---|---|---|
| **Contract source** | Rust trait (`#[rpc_module]`), no IDL | `.proto` IDL | TS router object | `.proto` IDL |
| **Cross-language** | Rust → Rust/Dart/TS | ~11 languages | TypeScript only | Go/TS/Swift/Kotlin/Dart… |
| **Client stays in sync via** | Codegen (Dart/TS from Rust source) | Codegen (`protoc`) | Type inference (zero codegen) | Codegen (`buf`) |
| **Wire format** | JSON (codec is a type param → binary pluggable, not shipped) | Protobuf binary | JSON | JSON **or** protobuf binary |
| **HTTP version** | h1 + h2c (server auto-negotiates) | HTTP/2 required | h1 (h2 via host) | h1 + h2 |
| **Client connection reuse** | Pooled | Pooled (channels) | Pooled | Pooled |
| **Streaming** | Server-push (progress frames) | Unary + client + server + **bidi** | Subscriptions (SSE/WS) | Unary + client + server + **bidi** |
| **Authorization** | Declarative `#[allowed_roles]`, enforced *before decode* | Interceptors (DIY) | Middleware (DIY) | Interceptors (DIY) |
| **Batteries (identity/host/state)** | In the box | RPC only | RPC only | RPC only |
| **Browser-native (no proxy)** | Yes | No (needs grpc-web) | Yes | Yes |
| **gRPC interop** | No | — (is gRPC) | No | Yes (a Connect server *is* a gRPC server) |
| **Schema governance (registry/lint/breaking-change)** | No | Partial (protoc/Buf ecosystem) | No | Yes (Buf Schema Registry) |
| **First-class Rust server** | Yes (the whole point) | Yes (tonic) | No | No |
| **Maturity** | 0.x, solo-maintained | CNCF standard | Mature (TS ecosystem) | Production, Buf-backed |

## The one-line take

- **vs gRPC** — gRPC wins on raw performance (protobuf binary + HTTP/2), polyglot
  reach, bidirectional streaming, and being an industry standard with mesh and
  observability tooling. allex wins on a Rust-native server with no IDL, browser
  and Flutter clients *without* a grpc-web proxy, authorization that's enforced for
  you, and the batteries. After the transport work described below, gRPC's edge
  over allex narrows to essentially **binary serialization + bidi streaming**.

- **vs tRPC** — tRPC is the best answer when the *whole stack is TypeScript*:
  zero-codegen, end-to-end type inference from server to client. But that magic is
  TS-only and effectively needs a monorepo. allex codegens precisely because its
  source of truth is Rust and its clients are polyglot — the case tRPC structurally
  can't serve (no Rust server, no first-class Dart client).

- **vs ConnectRPC** — the closest peer: also codegen-based, also JSON-capable and
  browser-native, and it has a Dart client. Connect's real advantages are binary
  wire format *today*, bidi streaming, schema governance (Buf Schema Registry), and
  gRPC interoperability. allex's decisive counter, in its lane, is a **real Rust
  server** (Connect has none), a **code-first contract with no IDL to maintain**,
  and **bundled identity / host / authorization**.

## Where allex is the answer

Pick allex when the backend is (or should be) **Rust**, you want the contract to
live in Rust with no separate schema artifact, and you need typed **Flutter and
browser** clients off that same contract — with identity, role-based
authorization, and host wiring already handled rather than assembled from parts.
None of gRPC, tRPC, or ConnectRPC give you a Rust server *and* generated Dart/TS
clients *and* the batteries together.

## Honest gaps

allex is a young, single-team project. The things it does **not** do (yet):

- **No binary wire format shipped.** The codec is a type parameter
  (`C: UnaryCodec`), so a compact binary codec is an additive change rather than a
  redesign — but today only `JsonCodec` ships. gRPC and Connect have binary now.
- **Streaming is server-push only** (a `ProgressableFuture` emits progress frames
  then a terminal result). gRPC and Connect do full bidirectional streaming.
- **No schema registry, lint, or breaking-change detection**, and **no gRPC
  interop** — a Connect server slots into the gRPC ecosystem; allex speaks only
  allex.
- **0.x and solo-maintained**, offered as-is with no SLA. See
  [Support & contributions](README.md#support--contributions).

## A note on performance and transport

The common "gRPC is faster" claim bundles several separate wins. In allex's stack:

- **The server already speaks HTTP/2.** It is served via `axum::serve`
  (axum + hyper-util's auto builder), which auto-negotiates HTTP/1.1 and HTTP/2
  cleartext (h2c) with no extra configuration.
- **Browsers already get HTTP/2 at the edge.** allex terminates no TLS in-process,
  so it runs behind a TLS-terminating reverse proxy; browser and Flutter clients
  negotiate h2 (and often h3) with that proxy independently of allex.
- **The Rust client pools connections.** The `HttpTransport` reqwest client is
  built once and reused, so calls share a keep-alive connection pool (and negotiate
  HTTP/2 via ALPN on any TLS hop).

What remains of gRPC's performance advantage is therefore **payload size and
serialization CPU** — i.e. protobuf's binary encoding — plus bidirectional
streaming. Both are on the additive-feature side of the ledger, not architectural
limits. For a typical app-backend workload (modest QPS, database-bound latency,
small-to-medium JSON payloads), the practical gap is small; it grows with
throughput, payload size, and mobile-bandwidth sensitivity.
