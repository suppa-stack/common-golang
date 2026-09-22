# Agent Guide for `common_golang`

This document is written for AI coding agents that need to work on the `suppa-ahg-stack/common-golang` Go module. The codebase is small but dense; read this guide before making changes.

## Project overview

`common_golang` is a shared Go library used by the backend services in the `suppa-ahg-stack`. It is not a standalone executable. It provides reusable packages for:

* Running HTTP servers with graceful shutdown.
* Server-Sent Events (SSE) fan-out with user/session/connection routing.
* A generic, fragment-based web application framework (the `app` package).
* Authentication helpers (Argon2 password hashing, service-to-service API keys, CSRF).
* The canonical `auth_app` wire client and the application-side session lifecycle.
* Notification domain logic and HTTP handlers.
* Reusable HTML components written in [templ](https://templ.guide/).
* Validation, structured logging, and internal HTTP client utilities.

The module path is `suppa-ahg-stack/common-golang` and it targets **Go 1.27.0**.

## Key configuration files

* `go.mod` / `go.sum` — standard Go module files.
* No `Makefile`, `Dockerfile`, `docker-compose.yml`, CI workflow, or package manager config exists. The project is built with the standard Go toolchain only.

### External dependencies (direct)

* `github.com/a-h/templ` — typed HTML templating; `.templ` files are compiled to Go.
* `github.com/jackc/pgx/v5` — PostgreSQL driver (used via `pgxpool.Pool` in the generic `App`).
* `github.com/invopop/ctxi18n` — request-scoped internationalisation.
* `github.com/lmittmann/tint` — coloured console logging.
* `github.com/go-mail/mail/v2` — SMTP email delivery.
* `github.com/joho/godotenv` — `.env` file loading.
* `golang.org/x/crypto` — Argon2id password hashing.

## Build and test commands

```bash
# Download dependencies and compile every package
go mod tidy
go build ./...

# Run the full test suite
go test ./...

# Regenerate templ components after editing .templ files
templ generate
```

`templ` must be installed to regenerate `ui/*_templ.go` files. The generated files are committed to the repository, so if you only edit `.go` files you do not need `templ`.

## Code organisation

```
app/                Generic web app framework (routes, fragments, SSE publishing, UI interactions)
authapp/            Canonical auth_app internal API DTOs and typed client
authsession/        Identity/role cache, CSRF, rotation and shared HTTP middleware
generalutil/        Small utilities: env loading, token generation, path resolution
logger/             Structured logger writing JSON to a file and coloured text to stdout
notifications/      Notification domain service, REST handlers, cursor pagination, SSE publisher interface
serverutil/         HTTP server utilities, middlewares, email, rate limiting, Argon2, internal API auth/client
  internalapi/      JSON request/response helpers, service auth, idempotency keys, resilient HTTP client
sse/                Typed SSE broker and HTTP handler
ui/                 templ components: confirm modal and notification bell
validator/          Struct-tag validator and simple validation helpers
```

## Main module divisions

### `app`

The centre of the framework. It defines a generic `App[TConfig, TQueries, TSessionService, TSseNames]` struct that consuming projects instantiate with their own config, generated SQLC queries, session service, and SSE event-name types.

Key abstractions:

* `RenderableComponent` / `PageComponent` — things that can render HTML into a CSS selector target.
* `FragmentPlanner` — decides which page fragments to refresh after navigation or an action.
* `NavigationHandlerWithPlanner` — `GET /unh` endpoint that updates the SSE page context and publishes fragment refreshes.
* `InteractionHandlerWithPlanner` — `POST /uih` endpoint that dispatches actions, validates CSRF, enforces roles/path access, and re-renders fragments.
* `EmailSender` interface — minimal surface for sending password-reset, OTP, and setup emails.

The app relies on the caller to provide session/CSRF/auth implementations via interfaces.

### `sse`

* `Broker` — pub/sub over typed `Event`s, indexed by connection ID, session ID, and user ID.
* `Handler` — HTTP handler that subscribes a client to one or more `EventHandler` streams and writes SSE events.
* Supports active-session filtering, per-connection filters, page-context tracking, and session/user ID re-association (used when auth rotates tokens while a connection stays open).

Delivery model, which callers must not work around:

* **One mailbox per HTTP connection.** The handler allocates the channel once and every broker of that connection joins it via `SubscribeWithMailbox`, so there is a single backpressure queue, no forwarding goroutine and no merge step per broker. `SubscribeWithConnectionID` still allocates its own channel and is kept for callers outside the handler.
* **Fan-out happens outside the lock.** `Publish*` snapshots its target connections under `mu.RLock`, releases, then sends. `PublishOptions.Filter` therefore runs *without* the broker lock and must stay cheap and non-blocking. Connection metadata (`PageContext`, `SessionID`, `UserID`) is held in atomic pointers for exactly this reason.
* **Mailboxes are never closed.** Publishers send without holding `mu`, so closing would race them into a panic. Readers stop on context cancellation.
* **A dropped event queues a resync.** When a mailbox is full the event is discarded and a single `system.resync` app-event replaces the queue head; the client re-requests its current page rather than replaying stale DOM fragments. Use `Stats()` and `ConnectionDrops()` for visibility — drops are also logged once per disconnect.
* **Session rotation.** `UpdateSessionID(old, new)` moves every connection of a session and needs no `X-SSE-Connection-ID`; prefer it over `UpdateConnectionSessionID` on rotation paths that have no such header. Forgetting to call it leaves connections indexed under a dead session, which the active-session filter then silently excludes from every publish.
* **Server timeouts.** The handler clears its read deadline (a `Server.ReadTimeout` firing mid-stream cancels the request context) and sets a rolling per-frame write deadline, so `ReadTimeout`/`WriteTimeout` can stay enabled process-wide.

### `serverutil`

* `ServerUtil` — creates and runs `http.Server` with graceful shutdown.
* `CspMiddleware` — sets a strict Content-Security-Policy with a per-request nonce, plus security headers.
* `RateLimiter` and `RateLimitMiddleware` — in-memory rate limiting keyed by IP + session cookie + user agent.
* `EmailSender` — configurable SMTP sender with validation; safe to construct with no config.
* Argon2 password hashing (`HashPassword`, `VerifyPassword`).
* `IsSpaRequest` and `PageRoute`.

### `serverutil/internalapi`

Shared infrastructure for JSON service-to-service APIs:

* `HTTPClient` — resilient outbound client with retries, request-ID propagation, `ApiKey` auth, and error classification.
* `ServiceAuth` — API-key middleware with scopes and expiry support.
* `ParseJSON`, `WriteJSON`, `WriteError` — strict JSON helpers (reject unknown fields, size-limited bodies).
* Idempotency key generation and validation.
* Request-ID and timeout middleware.

### `notifications`

* `Service` — create, list, mark-read notification operations with cursor pagination.
* `Store` interface — projects provide a SQLC-backed implementation.
* `Publisher` interface — abstracts SSE delivery of `notifications.created` / `notifications.read` events.
* `RegisterHandlers` — wires REST endpoints (`/notifications`, `/notifications/summary`, `/notifications/{id}/read`, etc.) into a mux.

### `ui`

`templ` components and rendering adapters intended to be embedded in consuming applications:

* `ConfirmModal` — DaisyUI/Alpine.js confirmation dialog.
* `NotificationBell` — notification dropdown driven by the REST endpoints above.
* `ModalToastContainer` — stable target for modal-scoped `ui.toast` events.
* `ComponentRenderer` — adapts dynamic render callbacks to `templ.Component`.

The generated `*_templ.go` files must stay in sync with the `.templ` sources.

### `authapp` and `authsession`

`authapp` owns the JSON contract used by the auth server and every consumer,
including idempotent user mutations and structured 404/409/unavailable errors.
`authsession` owns the bounded identity and role caches, singleflight cache
misses, fresh validation for mutations, token/CSRF transfer on rotation and
the shared HTTP middleware. Applications still select their role-error policy
and keep role gates and audit logging locally.

### `validator`

* `Validator[T]` — reflects over struct tags (`validation:"required,min:3,max:20"`) and collects validation errors.
* `ExtValidator` — simpler map-based validator helpers.
* Password policy: 8–32 characters, at least one uppercase, one lowercase, one digit, one special character.

### `logger`

`FileLogger` writes JSON logs to a configured file and coloured text to stdout using `lmittmann/tint`.

### `generalutil`

`LoadEnv` loads `.env.<env>` based on a `-env` flag. Also provides `GenerateToken` / `HashToken` and path resolution helpers.

## Code style guidelines

* Comments and exported identifiers are written in English.
* The validator package contains legacy French error-message strings; leave them as-is unless you are intentionally changing user-facing copy.
* Use `fmt.Errorf("...: %w", err)` for error wrapping.
* Prefer interfaces for boundaries that consuming projects will implement (`Store`, `Publisher`, `EmailSender`, `SessionValidator`, `CsrfChecker`, etc.).
* Use generics only where the existing `App` type already does; avoid introducing new generic surfaces for small helpers.
* The `App` type stores many generic parameters and callbacks by design — do not flatten it without a strong reason.
* Generated `*_templ.go` files must not be hand-edited; change the `.templ` source and run `templ generate`.

## Testing instructions

* Standard Go tests with `go test ./...`. The suite passes without external services (no database is required).
* HTTP handlers are tested with `net/http/httptest`.
* Store/publisher dependencies are mocked with hand-written fakes/spies (see `notifications/service_test.go`).
* The SSE broker has concurrency and race tests (see `sse/broker_test.go`).
* Logger tests create a temporary log file via `t.TempDir()`.

When adding features, add tests in the same package. Table-driven subtests are the dominant style.

## Security considerations

* **CSP**: `CspMiddleware` generates a per-request nonce and sets `default-src 'self'`, `script-src 'nonce-…' 'strict-dynamic'`, etc. UI tests in `ui/confirm_modal_test.go` enforce that components do not emit inline object construction forbidden by the CSP build.
* **CSRF**: Action endpoints require a CSRF token bound to the session cookie. The app relies on the caller to provide `CsrfSessionManager` / `CsrfChecker` implementations.
* **Rate limiting**: `RateLimitMiddleware` keys requests by client IP + session cookie + user agent. Call `RateLimiter.Stop()` in tests to avoid leaking goroutines.
* **Password hashing**: Uses Argon2id with conservative defaults (`Memory: 64 MiB`, `Iterations: 3`, `Parallelism: 2`).
* **Internal API auth**: `ServiceAuth` stores SHA-256 hashes of raw API keys and compares them in constant time. It supports scopes and key rotation overlap.
* **Cookies**: session/language cookies are `HttpOnly` and `SameSite=Lax`; the caller decides `Secure`.
* **Request bodies**: `internalapi.ParseJSON` caps bodies at 1 MiB and rejects unknown fields.
* **Idempotency keys**: validated to be 16–255 visible ASCII bytes; raw keys must never be logged.

## Deployment / release

This repository is a library, not a deployable service. Consuming Go modules import it and wire their own `main` packages, database pools, session stores, and route tables. There are no deployment manifests in this repository.
