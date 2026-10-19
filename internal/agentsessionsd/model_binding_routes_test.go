package agentsessionsd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func bindingRoutesConfigFile(t *testing.T, config string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "models.json")
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func startBindingRoutesDaemon(t *testing.T, args []string, opts ...Option) (*client.Client, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	opts = append(opts, withContext(ctx), withListening(func(addr string) { addrs <- addr }))
	go func() { done <- Run(args, opts...) }()
	var addr string
	select {
	case addr = <-addrs:
	case err := <-done:
		cancel()
		t.Fatalf("daemon before listening: %v", err)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("daemon startup timeout")
	}
	c, err := client.Dial(addr)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = c.Close()
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("daemon stop: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("daemon shutdown timeout")
			}
		})
	}
	t.Cleanup(stop)
	return c, stop
}

func TestRunModelBindingEnablesChatWithoutGlobalModel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Model  string `json:"model"`
			Stream bool   `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if request.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			data, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"role": "assistant", "content": request.Model}}}})
			_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", data)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": request.Model}}}})
	}))
	defer server.Close()
	config := bindingRoutesConfigFile(t, fmt.Sprintf(`{"version":1,"bindings":[{"name":"chat","endpoint":%q,"protocol":"chat-completions","default_model":"primary","allowed_models":["primary","chosen"]}]}`, server.URL+"/chat"))
	c, _ := startBindingRoutesDaemon(t, []string{"-addr", "127.0.0.1:0", "-journal", filepath.Join(t.TempDir(), "journal.db"), "-model-bindings", config})
	for _, model := range []string{"", "chosen"} {
		sess, err := c.CreateSession(t.Context(), &v1.Session{Harness: "chat", Model: model})
		if err != nil {
			t.Fatal(err)
		}
		result, err := c.Exec(t.Context(), client.ExecOptions{Session: sess.GetMetadata().GetUid(), Inputs: []string{"hello"}})
		want := model
		if want == "" {
			want = "primary"
		}
		if err != nil || result.Output != want {
			t.Fatalf("model %q turn=%+v,%v", model, result, err)
		}
	}
	if _, err := c.CreateSession(t.Context(), &v1.Session{Harness: "chat", Model: "forbidden"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid model: %v", err)
	}
	result, err := c.Exec(t.Context(), client.ExecOptions{Inputs: []string{"hello"}})
	if err != nil || result.Output != "echo:hello" {
		t.Fatalf("binding changed legacy default: %+v,%v", result, err)
	}
}
