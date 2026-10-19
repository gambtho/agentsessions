package agentsessionsd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-primitives/sessions/api"
)

type credentialRoundTripFunc func(*http.Request) (*http.Response, error)

func (f credentialRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// A transport must not place a credential in a caller-owned request (including on redirects),
// and its own origin check must precede every lookup, independently of Client.CheckRedirect.
func TestCredentialTransportClonesAndChecksOrigin(t *testing.T) {
	endpoint, _ := url.Parse("https://Example.test:443/x")
	var callbacks, requests int
	transport := credentialHTTPClient(endpoint, "api-key", func(context.Context) (string, error) { callbacks++; return "raw-key", nil }).Transport.(*credentialTransport)
	transport.base = credentialRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		if r.Header.Get("api-key") != "raw-key" {
			t.Error("credential missing")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})
	req, err := http.NewRequestWithContext(t.Context(), "POST", "https://example.test/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = transport.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if req.Header.Get("api-key") != "" {
		t.Fatal("caller request mutated")
	}
	for _, destination := range []string{"https://other.test/x", "http://example.test/x", "https://example.test:444/x"} {
		req.URL, _ = url.Parse(destination)
		if _, err = transport.RoundTrip(req); err == nil {
			t.Error("origin mismatch accepted")
		}
	}
	if callbacks != 1 || requests != 1 {
		t.Fatalf("wrong-origin lookup or dispatch: callbacks=%d requests=%d", callbacks, requests)
	}
}

func TestCredentialInvalidValueAndRemovalNeverDispatch(t *testing.T) {
	var received atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1) }))
	defer server.Close()
	for _, value := range []string{"", "PRIVATE\r\nInjected: token", "PRIVATE\x00", "PRIVATE\x7f"} {
		source := func(CredentialRef) (CredentialFunc, error) {
			return func(context.Context) (string, error) { return value, nil }, nil
		}
		bindings, err := loadModelBindings(bindingFile(t, bindingJSON(server.URL+"/x", `,"credential":{"type":"identity","name":"test"}`)), source)
		if err != nil {
			t.Fatal(err)
		}
		_, err = bindings["chat"].model(t.Context(), api.ModelRequest{})
		if err == nil || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("unsafe invalid-token result: %v", err)
		}
	}
	t.Setenv("BINDING_REMOVABLE_KEY", "available")
	bindings, err := loadModelBindings(bindingFile(t, bindingJSON(server.URL+"/x", `,"credential":{"type":"env","name":"BINDING_REMOVABLE_KEY"}`)), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("BINDING_REMOVABLE_KEY", "")
	if _, err = bindings["chat"].model(t.Context(), api.ModelRequest{}); err == nil {
		t.Fatal("removed env credential accepted")
	}
	if received.Load() != 0 {
		t.Fatalf("invalid credential sent %d requests", received.Load())
	}
}

func TestCredentialDeadlineReachesCallback(t *testing.T) {
	for _, protocol := range []string{"chat-completions", "responses"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", protocol, stream), func(t *testing.T) {
				started := make(chan struct{})
				release := make(chan struct{})
				source := func(CredentialRef) (CredentialFunc, error) {
					return func(ctx context.Context) (string, error) {
						close(started)
						select {
						case <-ctx.Done():
							return "", fmt.Errorf("PRIVATE: %w", ctx.Err())
						case <-release:
							return "", errors.New("test cleanup")
						}
					}, nil
				}
				config := strings.Replace(bindingJSON("http://localhost:1/x", `,"credential":{"type":"identity","name":"test"}`), "chat-completions", protocol, 1)
				bindings, err := loadModelBindings(bindingFile(t, config), source)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() {
					var err error
					if stream {
						_, err = bindings["chat"].stream(ctx, api.ModelRequest{}, nil)
					} else {
						_, err = bindings["chat"].model(ctx, api.ModelRequest{})
					}
					done <- err
				}()
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					close(release)
					t.Fatal("callback not reached")
				}
				select {
				case err := <-done:
					if !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "PRIVATE") {
						t.Fatalf("deadline lost: %v", err)
					}
				case <-time.After(5 * time.Second):
					close(release)
					t.Fatal("deadline ignored")
				}
			})
		}
	}
}

func TestBindingTLSFailurePrivateAndCredentialedHTTPRefused(t *testing.T) {
	const marker = "PRIVATE_REFERENCE_ENDPOINT"
	source := func(CredentialRef) (CredentialFunc, error) {
		return func(context.Context) (string, error) { return "token", nil }, nil
	}
	extra := `,"credential":{"type":"identity","name":"` + marker + `"}`
	if _, err := loadModelBindings(bindingFile(t, bindingJSON("http://"+marker+".test/x", extra)), source); err == nil || strings.Contains(err.Error(), marker) {
		t.Fatalf("unsafe HTTP refusal: %v", err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS accepted") }))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	for _, protocol := range []string{"chat-completions", "responses"} {
		config := strings.Replace(bindingJSON(server.URL+"/"+marker, extra), "chat-completions", protocol, 1)
		bindings, err := loadModelBindings(bindingFile(t, config), source)
		if err != nil {
			t.Fatal(err)
		}
		for _, stream := range []bool{false, true} {
			if stream {
				_, err = bindings["chat"].stream(t.Context(), api.ModelRequest{}, nil)
			} else {
				_, err = bindings["chat"].model(t.Context(), api.ModelRequest{})
			}
			if err == nil || strings.Contains(err.Error(), marker) {
				t.Fatalf("unsafe TLS error: %v", err)
			}
		}
	}
}
