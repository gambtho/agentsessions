package agentsessionsd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/model/openai"
)

func bindingFile(t *testing.T, data string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "bindings.json")
	if err := os.WriteFile(p, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}
func bindingJSON(endpoint, extra string) string {
	return `{"version":1,"bindings":[{"name":"chat","endpoint":` + fmt.Sprintf("%q", endpoint) + `,"protocol":"chat-completions","default_model":"primary","allowed_models":["primary","alternate"]` + extra + `}]}`
}
func TestBindingStrictConfig(t *testing.T) {
	valid := bindingJSON("https://example.test/custom?api-version=1", "")
	for name, data := range map[string]string{
		"version":         strings.Replace(valid, `"version":1`, `"version":2`, 1),
		"missing version": strings.Replace(valid, `"version":1,`, "", 1),
		"unknown":         strings.Replace(valid, `"version":1`, `"unknown":1,"version":1`, 1),
		"case":            strings.Replace(valid, `"version"`, `"Version"`, 1),
		"duplicate":       strings.Replace(valid, `"version":1`, `"version":1,"version":1`, 1),
		"null":            strings.Replace(valid, `"default_model":"primary"`, `"default_model":null`, 1),
		"trailing":        valid + ` {}`,
		"large":           valid + strings.Repeat(" ", 1<<20),
		"policy":          strings.Replace(valid, `"default_model":"primary"`, `"default_model":"other"`, 1),
		"empty allowed":   strings.Replace(valid, `["primary","alternate"]`, `[]`, 1),
		"empty name":      strings.Replace(valid, `"name":"chat"`, `"name":""`, 1),
		"duplicate name":  strings.Replace(valid, `}]}`, `},{"name":"chat","endpoint":"https://example.test/x","protocol":"responses","default_model":"primary","allowed_models":["primary"]}]}`, 1),
		"protocol":        strings.Replace(valid, `"chat-completions"`, `"auto"`, 1),
		"old env":         bindingJSON("https://example.test/x", `,"credential_env":"SECRET"`),
		"literal":         bindingJSON("https://example.test/x", `,"credential":"SECRET"`),
		"ref type":        bindingJSON("https://example.test/x", `,"credential":{"name":"SECRET"}`),
		"ref name":        bindingJSON("https://example.test/x", `,"credential":{"type":"env"}`),
		"ref unknown":     bindingJSON("https://example.test/x", `,"credential":{"type":"env","name":"SECRET","value":"SECRET"}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadModelBindings(bindingFile(t, data), nil); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
	if _, err := loadModelBindings(bindingFile(t, valid), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := loadModelBindings(bindingFile(t, valid+strings.Repeat(" ", (1<<20)-len(valid))), nil); err != nil {
		t.Fatal("exact limit refused:", err)
	}
}
func TestBindingEndpointAndAuthValidation(t *testing.T) {
	for _, endpoint := range []string{"/relative", "ftp://example.test/x", "https://example.test", "https://example.test//x", "https://user:secret@example.test/x", "https://example.test/x#secret", "https://example.test/x#", "https://example.test/x?", "https://example.test/%2f/x"} {
		t.Run(endpoint, func(t *testing.T) {
			if _, err := loadModelBindings(bindingFile(t, bindingJSON(endpoint, "")), nil); err == nil {
				t.Fatal("accepted invalid endpoint")
			}
		})
	}
	t.Setenv("BINDING_KEY", "token")
	for _, header := range []string{"bad header", "bad\r\nheader", "Content-Type", "content-LENGTH", "HOST", "Transfer-Encoding", "Connection", "TE", "Trailer", "Upgrade", "Proxy-Authorization", "Proxy-Connection", "Accept", "Accept-Encoding"} {
		extra := `,"credential":{"type":"env","name":"BINDING_KEY"},"auth_header":` + fmt.Sprintf("%q", header)
		if _, err := loadModelBindings(bindingFile(t, bindingJSON("https://example.test/x", extra)), nil); err == nil {
			t.Errorf("accepted header %q", header)
		}
	}
	if _, err := loadModelBindings(bindingFile(t, bindingJSON("https://example.test/x", `,"auth_header":"api-key"`)), nil); err == nil {
		t.Fatal("header without reference accepted")
	}
	for _, ref := range []CredentialRef{{Type: "file", Name: "marker"}, {Type: "env", Name: "BAD=NAME"}, {Type: "env", Name: "MISSING_BINDING_KEY"}} {
		if _, err := EnvCredentialSource(ref); err == nil {
			t.Fatal("invalid env reference accepted")
		}
	}
}
func TestBindingDistinctProvidersAndRefresh(t *testing.T) {
	t.Setenv("MODEL_API_KEY", "must-not-use")
	t.Setenv("OPENAI_API_KEY", "must-not-use")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		isResponses := strings.HasPrefix(r.URL.Path, "/responses")
		wantModel, wantHeader := "chat-model", "Bearer chat-key"
		if isResponses {
			wantModel, wantHeader = "responses-model", "responses-key"
		}
		if body["model"] != wantModel {
			t.Errorf("model=%v, want %s", body["model"], wantModel)
		}
		header := r.Header.Get("Authorization")
		if isResponses {
			header = r.Header.Get("api-key")
		}
		if header != wantHeader {
			t.Errorf("wrong credential for route")
		}
		if r.URL.RawQuery != "api-version=1" {
			t.Error("query lost")
		}
		calls.Add(1)
		if isResponses {
			if _, ok := body["messages"]; ok {
				t.Error("chat payload sent to responses")
			}
			final := `{"id":"response","status":"completed","output":[{"id":"m","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"responses","annotations":[]}]}]}`
			if body["stream"] == true {
				_, _ = fmt.Fprintf(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":%s}\n\n", final)
			} else {
				_, _ = io.WriteString(w, final)
			}
		} else {
			if _, ok := body["input"]; ok {
				t.Error("responses payload sent to chat")
			}
			if body["stream"] == true {
				_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"chat\"}}]}\n\ndata: [DONE]\n\n")
			} else {
				_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"chat"}}]}`)
			}
		}
	}))
	defer server.Close()
	config := `{"version":1,"bindings":[{"name":"chat","endpoint":"` + server.URL + `/chat?api-version=1","protocol":"chat-completions","default_model":"chat-model","allowed_models":["chat-model"],"credential":{"type":"identity","name":"chat"}},{"name":"other","endpoint":"` + server.URL + `/responses?api-version=1","protocol":"responses","default_model":"responses-model","allowed_models":["responses-model"],"credential":{"type":"identity","name":"other"},"auth_header":"api-key"}]}`
	var lookups atomic.Int32
	source := func(ref CredentialRef) (CredentialFunc, error) {
		return func(ctx context.Context) (string, error) {
			lookups.Add(1)
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			if ref.Name == "chat" {
				return "chat-key", nil
			}
			return "responses-key", nil
		}, nil
	}
	bindings, err := loadModelBindings(bindingFile(t, config), source)
	if err != nil {
		t.Fatal(err)
	}
	if lookups.Load() != 0 {
		t.Fatal("custom source prefetched token at startup")
	}
	for name, b := range bindings {
		want := "chat"
		if name == "other" {
			want = "responses"
		}
		if b.policy.Default == "" {
			t.Fatal("missing policy")
		}
		for _, stream := range []bool{false, true} {
			var out api.ModelResponse
			if stream {
				out, err = b.stream(t.Context(), api.ModelRequest{}, nil)
			} else {
				out, err = b.model(t.Context(), api.ModelRequest{})
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Message.Parts) != 1 || out.Message.Parts[0].Text.Text != want {
				t.Fatalf("wrong response: %+v", out)
			}
		}
	}
	if calls.Load() != 4 || lookups.Load() != 4 {
		t.Fatalf("requests=%d callbacks=%d", calls.Load(), lookups.Load())
	}
}
func TestBindingAnonymousAndEnvRotation(t *testing.T) {
	t.Setenv("MODEL_API_KEY", "not-for-anonymous")
	t.Setenv("OPENAI_API_KEY", "not-for-anonymous")
	headers := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`)
	}))
	defer server.Close()
	b, err := loadModelBindings(bindingFile(t, bindingJSON(server.URL+"/x", "")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b["chat"].model(t.Context(), api.ModelRequest{}); err != nil {
		t.Fatal(err)
	}
	if <-headers != "" {
		t.Fatal("anonymous used ambient key")
	}
	t.Setenv("BINDING_KEY", "first")
	b, err = loadModelBindings(bindingFile(t, bindingJSON(server.URL+"/x", `,"credential":{"type":"env","name":"BINDING_KEY"},"auth_header":"authorization"`)), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"first", "second"} {
		t.Setenv("BINDING_KEY", token)
		if _, err = b["chat"].model(t.Context(), api.ModelRequest{}); err != nil {
			t.Fatal(err)
		}
		if <-headers != "Bearer "+token {
			t.Fatal("credential not refreshed")
		}
	}
}
func TestBindingPrivateErrors(t *testing.T) {
	const marker = "PRIVATE_ENDPOINT_REFERENCE_TOKEN"
	raw := fmt.Errorf("%s: %w", marker, errors.New(marker))
	extra := `,"credential":{"type":"identity","name":"` + marker + `"}`
	for _, source := range []CredentialSource{
		func(CredentialRef) (CredentialFunc, error) { return nil, raw },
		func(CredentialRef) (CredentialFunc, error) { return nil, nil },
	} {
		_, err := loadModelBindings(bindingFile(t, bindingJSON("https://example.test/"+marker, extra)), source)
		if err == nil || strings.Contains(fmt.Sprintf("%+v", err), marker) || errors.Unwrap(err) != nil {
			t.Fatalf("unsafe setup error: %v", err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(400); _, _ = io.WriteString(w, marker) }))
	defer server.Close()
	for _, protocol := range []string{"chat-completions", "responses"} {
		for _, callbackErr := range []error{nil, raw, fmt.Errorf("%s: %w", marker, context.Canceled), fmt.Errorf("%s: %w", marker, context.DeadlineExceeded)} {
			source := func(CredentialRef) (CredentialFunc, error) {
				return func(context.Context) (string, error) { return "token", callbackErr }, nil
			}
			config := strings.Replace(bindingJSON(server.URL+"/"+marker+"?private="+marker, extra), "chat-completions", protocol, 1)
			b, err := loadModelBindings(bindingFile(t, config), source)
			if err != nil {
				t.Fatal(err)
			}
			for _, stream := range []bool{false, true} {
				if stream {
					_, err = b["chat"].stream(t.Context(), api.ModelRequest{}, nil)
				} else {
					_, err = b["chat"].model(t.Context(), api.ModelRequest{})
				}
				if err == nil || strings.Contains(fmt.Sprintf("%+v", err), marker) {
					t.Fatalf("unsafe runtime error: %v", err)
				}
				if errors.Is(callbackErr, context.Canceled) && !errors.Is(err, context.Canceled) {
					t.Fatal("lost cancellation")
				}
				if errors.Is(callbackErr, context.DeadlineExceeded) && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("lost deadline")
				}
			}
		}
	}
	b, err := loadModelBindings(bindingFile(t, bindingJSON(server.URL+"/x", "")), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b["chat"].model(t.Context(), api.ModelRequest{Messages: []api.Message{{Role: "user", Parts: []api.Part{{}}}}})
	if !errors.Is(err, openai.ErrUnsupportedPart) {
		t.Fatalf("unsupported part identity lost: %v", err)
	}
}
func TestBindingCredentialCancellationAndTimeout(t *testing.T) {
	for _, protocol := range []string{"chat-completions", "responses"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", protocol, stream), func(t *testing.T) {
				started := make(chan context.Context, 1)
				source := func(CredentialRef) (CredentialFunc, error) {
					return func(ctx context.Context) (string, error) {
						started <- ctx
						<-ctx.Done()
						return "", fmt.Errorf("private: %w", ctx.Err())
					}, nil
				}
				config := strings.Replace(bindingJSON("http://localhost:1/x", `,"credential":{"type":"identity","name":"test"}`), "chat-completions", protocol, 1)
				b, err := loadModelBindings(bindingFile(t, config), source)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					var err error
					if stream {
						_, err = b["chat"].stream(ctx, api.ModelRequest{}, nil)
					} else {
						_, err = b["chat"].model(ctx, api.ModelRequest{})
					}
					done <- err
				}()
				select {
				case requestCtx := <-started:
					deadline, ok := requestCtx.Deadline()
					remaining := time.Until(deadline)
					if !ok || remaining < 119*time.Second || remaining > 120*time.Second {
						t.Errorf("HTTP timeout not preserved: %v", remaining)
					}
					cancel()
				case <-time.After(5 * time.Second):
					t.Fatal("callback not reached")
				}
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation lost: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("callback did not cancel")
				}
			})
		}
	}
}
func TestBindingRedirectCredentialIsolation(t *testing.T) {
	var received, lookups atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1) }))
	defer target.Close()
	cross := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/x" {
			where := "/ok"
			if cross {
				where = target.URL + "/ok"
			}
			http.Redirect(w, r, where, http.StatusTemporaryRedirect)
			return
		}
		if r.Header.Get("Authorization") != "Bearer refreshed-2" {
			t.Error("redirect token not refreshed")
		}
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`)
	}))
	defer server.Close()
	source := func(CredentialRef) (CredentialFunc, error) {
		return func(context.Context) (string, error) { return fmt.Sprintf("refreshed-%d", lookups.Add(1)), nil }, nil
	}
	b, err := loadModelBindings(bindingFile(t, bindingJSON(server.URL+"/x", `,"credential":{"type":"identity","name":"x"}`)), source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b["chat"].model(t.Context(), api.ModelRequest{}); err != nil {
		t.Fatal(err)
	}
	cross = true
	if _, err = b["chat"].model(t.Context(), api.ModelRequest{}); err == nil {
		t.Fatal("cross-origin redirect accepted")
	}
	if received.Load() != 0 || lookups.Load() != 3 {
		t.Fatalf("redirect leaked request or looked up credential: requests=%d lookups=%d", received.Load(), lookups.Load())
	}
}
