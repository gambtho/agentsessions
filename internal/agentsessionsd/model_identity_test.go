package agentsessionsd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/agent-primitives/sessions/client"
)

func TestRunModelBindingIdentitySourceRefreshesResponsesTokens(t *testing.T) {
	var issued, received atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := received.Add(1)
		if r.Header.Get("Authorization") != fmt.Sprintf("Bearer identity-%d", n) {
			t.Error("identity token was not refreshed")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if _, chat := body["messages"]; chat || body["model"] != "m" {
			t.Error("wrong protocol/model reached identity endpoint")
		}
		final := `{"id":"response","status":"completed","output":[{"id":"msg","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"identity","annotations":[]}]}]}`
		if body["stream"] == true {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":%s}\n\n", final)
			return
		}
		_, _ = io.WriteString(w, final)
	}))
	defer server.Close()
	source := func(ref CredentialRef) (CredentialFunc, error) {
		if ref.Type != "workload-identity" || ref.Name != "models" {
			return nil, fmt.Errorf("unexpected identity reference")
		}
		return func(ctx context.Context) (string, error) {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			return fmt.Sprintf("identity-%d", issued.Add(1)), nil
		}, nil
	}
	config := bindingRoutesConfigFile(t, fmt.Sprintf(`{"version":1,"bindings":[{"name":"chat","endpoint":%q,"protocol":"responses","default_model":"m","allowed_models":["m"],"credential":{"type":"workload-identity","name":"models"}}]}`, server.URL+"/responses"))
	c, _ := startBindingRoutesDaemon(t, []string{"-addr", "127.0.0.1:0", "-journal", filepath.Join(t.TempDir(), "journal.db"), "-model-bindings", config}, WithCredentialSource(source))
	if issued.Load() != 0 {
		t.Fatal("startup prefetched an identity token")
	}
	for range 2 {
		turn, err := c.Exec(t.Context(), client.ExecOptions{Harness: "chat", Inputs: []string{"hello"}})
		if err != nil || turn.Output != "identity" {
			t.Fatalf("identity-backed turn: %+v,%v", turn, err)
		}
	}
	if issued.Load() != 2 || received.Load() != 2 {
		t.Fatalf("identity dispatch counts %d/%d", issued.Load(), received.Load())
	}
}
