package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/agent-primitives/sessions/api/genpb"
	"github.com/agent-primitives/sessions/client"
	"github.com/agent-primitives/sessions/sqlitelog"
)

// A provider/source diagnostic is not trusted to keep the credential private. Exercise the real
// binary's status, durable ERROR, and stderr boundary, rather than only the config compiler.
func TestModelBindingCredentialsNeverReachAPIJournalOrLogs(t *testing.T) {
	const key = "test-only-private-credential-marker"
	const reference = "MODEL_BINDING_PRIVATE_REFERENCE_MARKER"
	const endpointMarker = "private-endpoint-marker"
	t.Setenv(reference, key)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+key {
			t.Error("missing bound credential")
		}
		http.Error(w, key+" "+reference+" "+endpointMarker, http.StatusBadGateway)
	}))
	defer server.Close()
	bin := buildBinaries(t)
	path := filepath.Join(t.TempDir(), "journal.db")
	config := filepath.Join(t.TempDir(), "models.json")
	data := fmt.Sprintf(`{"version":1,"bindings":[{"name":"chat","endpoint":%q,"protocol":"chat-completions","default_model":"m","allowed_models":["m"],"credential":{"type":"env","name":%q}}]}`, server.URL+"/chat?"+endpointMarker+"=1", reference)
	if err := os.WriteFile(config, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	addr := freeLoopbackAddr(t)
	proc := startServer(t, bin, addr, "-journal", path, "-model-bindings", config)
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	sess, err := c.CreateSession(ctx, &v1.Session{Harness: "chat"})
	if err != nil {
		t.Fatal(err)
	}
	uid := sess.GetMetadata().GetUid()
	_, err = c.Exec(ctx, client.ExecOptions{Session: uid, Inputs: []string{"hello"}})
	if err == nil {
		t.Fatal("provider failure was lost")
	}
	public := err.Error() + sess.String()
	events, replayErr := c.Replay(ctx, uid, 0, 0)
	if replayErr != nil {
		t.Fatal(replayErr)
	}
	for _, event := range events {
		public += event.String()
	}
	proc.stop(t) // Wait before reading the process buffer or SQLite checkpoint.
	logs := proc.out.String()
	store, err := sqlitelog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Session(uid).Verify(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{key, reference, endpointMarker} {
		if strings.Contains(public, marker) || strings.Contains(logs, marker) || strings.Contains(string(raw), marker) {
			t.Fatal("private binding data escaped its host-only boundary")
		}
	}
}
