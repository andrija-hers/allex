# Security Policy

allex includes an identity/authentication system (`allex-identity` and its
stores, plus the token and bearer-validation paths in `allex-transport` /
`allex-serve`). Security reports about those — or any crate — are taken
seriously, within the limits of a small, best-effort project.

## Supported versions

allex is pre-1.0. Only the **latest published version** receives fixes. There is
no back-porting to older `0.x` releases.

## Reporting a vulnerability

**Please do not open a public issue for a security vulnerability.**

Report it privately by email to **andrija.hers@gmail.com**. Include, as best you can:

- the affected crate and version,
- a description of the issue and its impact,
- steps or a proof-of-concept to reproduce it.

You will get an acknowledgement as soon as is practical. This is a solo,
best-effort project: there is **no guaranteed response time, no SLA, and no bug
bounty**. Credible reports will be investigated and, where warranted, fixed and
disclosed with credit to the reporter (unless you prefer to remain anonymous).

## Security model — know these before relying on allex

These are deliberate design properties, documented so you can judge fit. See
[AGENTS.md](AGENTS.md) for the full reasoning.

- **Sessions are in-memory ⇒ effectively single-instance.** Refresh-token
  rotation and logout-revocation live in process memory and do **not** cross
  instances. Access-token *validation* is stateless, but a token revoked on one
  host still validates on another until it expires. Run a single instance until a
  durable session store exists.
- **A JWT is signed, not encrypted.** The client can read the token payload,
  including the opaque `profile` claim. Put only authorization data the user is
  allowed to see there — never secrets.
- **Rights are frozen into the access token at issue time.** Changes propagate at
  the refresh boundary (staleness ≤ the access-token TTL). For a right that must
  be revoked instantly, re-check it per request inside the sensitive method
  rather than trusting the token.
- **Public registration can self-elevate if unguarded.** A raw `register` trusts
  the client's requested role. Wrap the store in
  `identity::RegistrationGuard::force_role(store, "user")` before exposing it, and
  provision privileged accounts out of band with `ensure_user`.

## Scope

allex provides mechanisms; correct deployment is the operator's responsibility.
Misconfiguration (e.g. a leaked JWT secret, an unguarded `register`, running
multiple instances against the in-memory session store) is outside the scope of a
vulnerability report, though documentation improvements to prevent such mistakes
are always welcome.
