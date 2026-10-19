package agentsessionsd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/agent-primitives/sessions/api"
	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/client"
	"github.com/agent-primitives/sessions/harness/chatagent"
	"github.com/agent-primitives/sessions/harness/echoagent"
	"github.com/agent-primitives/sessions/placement"
	"github.com/agent-primitives/sessions/runtime/local"
	"github.com/agent-primitives/sessions/session"
	"github.com/agent-primitives/sessions/sqlitelog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestModelBindingsSelectStaticRemoteAndExtraProviders(t *testing.T) {
	address := serveHarness(t, chatagent.Harness{Model: "declared"})
	extra := local.New(chatagent.Harness{Model: "declared"})
	defer func() { _ = extra.Close() }()
	var plain, stream atomic.Int32
	bindings := modelBindings{
		"remote": {policy: api.ModelPolicy{Default: "remote-model", Allowed: []string{"remote-model", "declared"}}, model: func(_ context.Context, r api.ModelRequest) (api.ModelResponse, error) {
			plain.Add(1)
			return api.ModelResponse{Message: *api.TextMessage("assistant", "remote:"+r.Model)}, nil
		}},
		"extra": {policy: api.ModelPolicy{Default: "extra-model", Allowed: []string{"extra-model", "declared"}}, stream: func(_ context.Context, r api.ModelRequest, chunk func(string)) (api.ModelResponse, error) {
			stream.Add(1)
			text := "extra:" + r.Model
			chunk(text)
			return api.ModelResponse{Message: *api.TextMessage("assistant", text)}, nil
		}},
	}
	registry, closeBackends, err := harnessRegistry("", remoteHarnesses{"remote": address}, nil, nil, nil, nil, map[string]placement.Backend{"extra": extra}, bindings)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBackends()
	if registry.Has("chat") {
		t.Fatal("unrelated binding enabled chat")
	}
	c := newTestClient(t, registry)
	for _, name := range []string{"remote", "extra"} {
		turn, err := c.Exec(t.Context(), client.ExecOptions{Harness: name, Inputs: []string{"hello"}})
		if err != nil || turn.Output != name+":"+name+"-model" {
			t.Fatalf("%s selected wrong provider: %+v,%v", name, turn, err)
		}
		if _, err := c.CreateSession(t.Context(), &v1.Session{Harness: name, Model: "other"}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%s policy absent: %v", name, err)
		}
	}
	if plain.Load() != 1 || stream.Load() != 1 {
		t.Fatalf("plain/stream dispatch counts %d/%d", plain.Load(), stream.Load())
	}
	if err := checkRemotes("", remoteHarnesses{"unbound": address}, bindings); err == nil {
		t.Fatal("unbound remote silently used echo")
	}
	if err := checkBackendHarnesses("", nil, []string{"unbound"}, bindings); err == nil {
		t.Fatal("unbound extra silently used echo")
	}
}

func bindingLifecycleProvider(t *testing.T, wantCredential string, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+wantCredential {
			t.Error("provider got wrong credential")
			http.Error(w, "refused", http.StatusForbidden)
			return
		}
		var req struct {
			Model  string `json:"model"`
			Stream bool   `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		calls.Add(1)
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			data, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"role": "assistant", "content": req.Model}}}})
			_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", data)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": req.Model}}}})
	}))
	t.Cleanup(server.Close)
	return server
}

func TestModelBindingRegistrationReloadAndPrivacy(t *testing.T) {
	const ref = "BINDING_PRIVATE_REF_MARKER"
	const key = "binding-secret-value-marker"
	t.Setenv(ref, key)
	var calls atomic.Int32
	provider := bindingLifecycleProvider(t, key, &calls)
	address := serveHarness(t, chatagent.Harness{Model: "declared"})
	endpoint := provider.URL + "/chat?private_endpoint_marker=1"
	config := bindingRoutesConfigFile(t, fmt.Sprintf(`{"version":1,"bindings":[{"name":"registered","endpoint":%q,"protocol":"chat-completions","default_model":"primary","allowed_models":["primary","chosen","declared"],"credential":{"type":"env","name":%q}},{"name":"rejected","endpoint":%q,"protocol":"chat-completions","default_model":"other","allowed_models":["other"]}]}`, endpoint, ref, provider.URL+"/chat"))
	bindings, err := loadModelBindings(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(t.TempDir(), "journal.db")
	store, err := sqlitelog.Open(journal)
	if err != nil {
		t.Fatal(err)
	}
	registry, closeBackends, err := harnessRegistry("", nil, echoagent.Model, nil, nil, nil, nil, bindings)
	if err != nil {
		t.Fatal(err)
	}
	operator, err := session.NewHarnessRegistry(store, registry, registeredPlacer("", echoagent.Model, nil, nil, bindings))
	if err != nil {
		t.Fatal(err)
	}
	spec := &v1.HarnessSpec{DescriptorId: "chat", Capabilities: &v1.Capabilities{Resumability: v1.Resumability_RESUMABILITY_STATELESS_REPLAY}, Placement: &v1.HarnessSpec_Remote{Remote: &v1.RemotePlacement{Address: address}}}
	if _, err := operator.RegisterHarness(t.Context(), &v1.RegisterHarnessRequest{Name: "rejected", Spec: spec}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("descriptor outside policy admitted: %v", err)
	}
	if _, err := store.Harness("rejected"); !errors.Is(err, sqlitelog.ErrHarnessNotFound) {
		t.Fatal("refused registration persisted")
	}
	registered, err := operator.RegisterHarness(t.Context(), &v1.RegisterHarnessRequest{Name: "registered", Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{key, ref, "private_endpoint_marker"} {
		if strings.Contains(registered.String(), marker) {
			t.Fatal("registry API exposed binding configuration")
		}
	}
	operator.Close()
	closeBackends()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	args := []string{"-addr", "127.0.0.1:0", "-journal", journal, "-model-bindings", config}
	c, stop := startBindingRoutesDaemon(t, args)
	sess, err := c.CreateSession(t.Context(), &v1.Session{Harness: "registered", Model: "chosen"})
	if err != nil {
		t.Fatal(err)
	}
	uid := sess.GetMetadata().GetUid()
	first, err := c.Exec(t.Context(), client.ExecOptions{Session: uid, Inputs: []string{"one"}})
	if err != nil || first.Output != "chosen" {
		t.Fatalf("registered binding first turn: %+v,%v", first, err)
	}
	stop()
	c, stop = startBindingRoutesDaemon(t, args)
	second, err := c.Exec(t.Context(), client.ExecOptions{Session: uid, Inputs: []string{"two"}, ExpectedLastSeq: &first.LastSeq})
	if err != nil || second.Output != "chosen" || calls.Load() != 2 {
		t.Fatalf("reloaded binding/replay: %+v,%v,calls=%d", second, err, calls.Load())
	}
	events, err := c.Replay(t.Context(), uid, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var records strings.Builder
	for _, record := range events {
		records.WriteString(record.String())
	}
	if calls.Load() != 2 {
		t.Fatal("journal replay invoked provider")
	}
	for _, marker := range []string{key, ref, "private_endpoint_marker"} {
		if strings.Contains(records.String(), marker) || strings.Contains(second.Session.String(), marker) {
			t.Fatal("Sessions API/journal exposed private binding data")
		}
	}
	stop()
	store, err = sqlitelog.Open(journal)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Session(uid).Verify(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(journal)
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{key, ref, "private_endpoint_marker"} {
		if strings.Contains(string(raw), marker) {
			t.Fatal("SQLite persisted private binding data")
		}
	}
}

func TestRunInvalidModelBindingLeavesNoJournal(t *testing.T) {
	for _, config := range []string{`{"version":2}`, `{"version":1,"bindings":[{"name":"chat","endpoint":"https://bad.example/chat","protocol":"auto","default_model":"m","allowed_models":["m"]}]}`} {
		journal := filepath.Join(t.TempDir(), "journal.db")
		if err := Run([]string{"-journal", journal, "-model-bindings", bindingRoutesConfigFile(t, config)}); err == nil {
			t.Fatal("invalid config accepted")
		}
		if _, err := os.Stat(journal); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid config created journal: %v", err)
		}
	}
	address := serveHarness(t, chatagent.Harness{Model: "forbidden"})
	config := bindingRoutesConfigFile(t, `{"version":1,"bindings":[{"name":"remote","endpoint":"https://models.example/chat","protocol":"chat-completions","default_model":"m","allowed_models":["m"]}]}`)
	journal := filepath.Join(t.TempDir(), "journal.db")
	if err := Run([]string{"-journal", journal, "-harness", "remote=" + address, "-model-bindings", config}); err == nil {
		t.Fatal("descriptor mismatch admitted")
	}
	if _, err := os.Stat(journal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed static policy admission created journal: %v", err)
	}
}
