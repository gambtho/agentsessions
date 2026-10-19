# Per-harness model bindings

`agentsessionsd -model-bindings /path/models.json` selects a host-owned model provider and model
policy by harness **route name**, not by `HarnessDescriptor.id`. Static `-harness` routes, built-ins,
stored registrations and `WithHarnessBackends` routes use the same lookup. A binding does not create
or reserve a harness; it may name a future registration.

## Configuration

The file is strict JSON, at most 1 MiB, with version 1. Unknown or duplicate fields, case variants,
invalid UTF-8 and ambiguous values are refused. Names must be nonempty and unique.

```json
{
  "version": 1,
  "bindings": [
    {
      "name": "chat",
      "endpoint": "https://models.example/v1/responses",
      "protocol": "responses",
      "default_model": "primary",
      "allowed_models": ["primary", "alternate"],
      "credential": {"type": "env", "name": "CHAT_MODEL_KEY"},
      "auth_header": "Authorization"
    }
  ]
}
```

```bash
agentsessionsd --journal /tmp/bound.db --model-bindings /path/models.json
CHAT=$(agentctl create --server 127.0.0.1:8080 --harness chat)
agentctl exec --server 127.0.0.1:8080 --session "$CHAT" --input "name three primes"
```

- `endpoint` is the **full request URL**, not the prefix used by legacy `-model-base-url`. It must
  be absolute HTTP(S), with a host and nonempty path, without userinfo or a fragment. Empty paths,
  double-leading-slash paths and normalization that changes the configured endpoint are refused.
  Nonsecret query parameters such as `api-version` are supported. Never put credentials in URLs.
- `protocol` must be exactly `chat-completions` or `responses`. It selects the corresponding
  OpenAI-compatible adapter explicitly: no inference, retries or protocol/model fallback. Adapter
  request, streaming, tool and reasoning limits still apply; see [Responses](responses-adapter.md).
- `default_model` must be explicit, nonempty and in `allowed_models`; IDs are opaque. Registration
  checks that every declared descriptor model is allowed. An empty declaration is valid and does
  not widen the policy. A binding for `echo` must explicitly allow its declared `echo` ID too.
- `credential` is an optional typed reference with nonempty `type` and `name`. The stock daemon
  implements **only `env`**. The named variable must exist, be nonempty and be a valid header value
  at startup; it is read again for every HTTP attempt. Missing or invalid values later refuse the
  request without sending it. This never falls back to `MODEL_API_KEY` or `OPENAI_API_KEY`.
  Omitting `credential` means anonymous, even if global key variables are set. Literal keys and
  file references are not accepted.
- `auth_header` defaults to `Authorization`, which sends `Bearer <raw token>`. Another valid header
  such as `api-key` sends the raw value. Names are case-insensitive. Explicit headers without a
  credential reference and body/framing headers (`Host`, `Content-Type`, `Content-Length`,
  `Transfer-Encoding`, connection/proxy headers, `TE`, `Trailer`, `Upgrade`, `Accept`,
  `Accept-Encoding`) are refused. Invalid credential/header data is never reflected in errors.

With credentials configured, HTTPS is required except for the existing loopback/localhost and
`*.svc`/`*.svc.cluster.local` trusted-network exceptions. A service-name check does not prove the
traffic stays inside Kubernetes; it still sends credentials and prompts in cleartext on that
network. Prefer HTTPS. Clients refuse cross-origin redirects before obtaining another credential,
retain the two-minute HTTP timeout, and propagate cancellation/deadlines to credential lookup.

Invalid configuration and static descriptor admission fail before opening the journal or serving
listeners. Extra-backend factories retain their existing lifecycle and may already have side
effects before descriptor admission; they must supply their cleanup. A failed stored registration
stays unavailable on this host until restart without disabling other routes.

## Routing, compatibility and restart

A bound remote/extra route needs no global `-model`. An **unbound** remote/extra route still requires
`-model`, as before; it never silently uses the echo stub. A `chat` binding enables chat without
`-model`; a binding for another name does not. Echo remains the default harness. Existing global
flags keep their behavior for unbound routes. Configuration is loaded on restart, not hot-reloaded.

New sessions on a bound route opt into model admission. Empty `Session.model` selects that route's
explicit default for a new turn; nonempty values and per-call overrides must be allowed. The
requested metadata stays empty when requested empty. `Start.Model` and `ExecutionStart.model`
carry the effective default; recorded per-call IDs and hashes are unchanged.

**Removing a binding does not turn opted-in sessions into legacy sessions.** If their selected
served route has no policy, normal Exec, no-input approval recovery and Resume refuse with
`FAILED_PRECONDITION` before compute creation/restoration, fencing, repair or journal changes.
Recovery checks the pending turn's recorded route, which may differ from the metadata route. Idle
Resume is also refused. Entirely unserved routes keep their normal routing errors. Journal replay
and reading, Suspend and Fork remain available; fork children inherit the eligibility bit. Legacy
sessions keep legacy model semantics, and unaffected routes keep working. Existing prerequisites
for an unbound remote provider still apply; removing both its binding and global `-model` does not
make the echo stub a valid fallback.

On restart, current default/allowed IDs and provider settings govern new turns, new live effects
and unanswered-intent re-drives. A changed default does not rewrite recorded execution defaults;
recovery prefixes use recorded effects and defaults. Narrowing allowed IDs can refuse later work
or descriptor admission. Endpoint/protocol/credential changes affect new outbound requests, not
historical replay; no endpoint or secret is pinned in a session. No schema migration is added.

## Host-supplied credentials, including workload identity

Repository-owned hosts using `internal/agentsessionsd.Run` can provide `WithCredentialSource`, the
same option style as `WithHarnessBackends`. This is a source **factory**, not a daemon-lifetime
snapshot of a token. It validates the typed reference and returns a concurrency-safe,
context-aware callback invoked once per HTTP attempt, including allowed same-origin redirects.
A custom factory replaces the default; delegate `env` explicitly if both are wanted.

<!-- doccheck: file -->
```go
package main

import (
    "context"
    "errors"

    "github.com/agent-primitives/sessions/internal/agentsessionsd"
)

type TokenProvider interface {
    Token(context.Context, string) (string, error)
}

func identitySource(provider TokenProvider) agentsessionsd.CredentialSource {
    return func(ref agentsessionsd.CredentialRef) (agentsessionsd.CredentialFunc, error) {
        if ref.Type == "env" {
            return agentsessionsd.EnvCredentialSource(ref)
        }
        if ref.Type != "workload-identity" {
            return nil, errors.New("unsupported credential source")
        }
        return func(ctx context.Context) (string, error) {
            return provider.Token(ctx, ref.Name)
        }, nil
    }
}

func run(args []string, provider TokenProvider) error {
    return agentsessionsd.Run(args, agentsessionsd.WithCredentialSource(identitySource(provider)))
}
```

The host owns its identity SDK client, cache and cleanup; `name` can select a host-configured
resource/audience. The source should validate that selection at setup and honor cancellation during
lookup. An AKS host can return an SDK-acquired access token for a reference such as
`{"type":"workload-identity","name":"models"}` without storing keys. The stock daemon rejects this
type; this example supplies the seam, not an identity SDK implementation or live AKS qualification.
There is no token prefetch or network probe for custom sources at daemon startup. Header validity
and destination checks precede sending a credential. Sources must not log credentials themselves.

## Privacy and operational limits

Binding credential references/values, endpoints and complete config objects are never serialized
into Sessions/HarnessRegistry responses, journal events or binding logs. Source setup and request
failures discard private messages and wrapped causes. Bound provider failures likewise expose only
fixed categories, including safe cancellation/deadline/unsupported-part identities; HTTP response
messages, query values and raw transport details are not forwarded to durable diagnostics. This
boundary also discards HTTP status and provider `error.code`/`error.param`, rather than parsing
adapter error strings or risking private payloads. This trades provider diagnostic detail for the
host-only credential boundary. Legacy unbound adapter
error behavior is unchanged. Normal successful provider-generated model content is still model
content, not arbitrary-content DLP.

The source callback is trusted host code, not a Sessions API hook. Clients cannot supply credential
references, endpoints or provider settings. The daemon remains unauthenticated by default; these
bindings are not tenant authorization, network isolation or a production identity deployment.
