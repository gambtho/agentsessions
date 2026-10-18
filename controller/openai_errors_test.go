package controller_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-primitives/sessions/api"
	"github.com/agent-primitives/sessions/controller"
	"github.com/agent-primitives/sessions/model/openai"
	"github.com/agent-primitives/sessions/sqlitelog"
)

// Provider messages and non-JSON gateway bodies must not cross the durable journal boundary.
func TestChatHTTPErrorJournalPrivacy(t *testing.T) {
	const secret = "sk-fixture-do-not-journal"
	for _, body := range []string{
		`{"error":{"param":"messages[0].content","message":"` + secret + `","code":"invalid_value"}}`,
		"gateway failure: " + secret,
	} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("json=%t/stream=%t", strings.HasPrefix(body, "{"), streaming), func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, body)
				}))
				defer srv.Close()
				client, err := openai.New(openai.WithBaseURL(srv.URL), openai.WithModel("test-model"))
				if err != nil {
					t.Fatal(err)
				}
				store, err := sqlitelog.Open(filepath.Join(t.TempDir(), "journal.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				log := store.Session("s")
				var opts []controller.Option
				if streaming {
					opts = append(opts, controller.WithStreamingModel(client.StreamModel))
				}
				c, err := controller.New(log, client.Model, opts...)
				if err != nil {
					t.Fatal(err)
				}
				err = c.Exec(t.Context(), &echoHarness{}, []api.Message{msg("hello")}, 0)
				want := "openai: test-model: http 400"
				if strings.HasPrefix(body, "{") {
					want += " (code=invalid_value, param=messages[0].content)"
				}
				if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), secret) {
					t.Errorf("Exec error = %v, want payload-free diagnostic %q", err, want)
				}
				records, err := log.Read(1)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := json.Marshal(records)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(raw), secret) || !strings.Contains(string(raw), want) {
					t.Fatalf("journal must contain the safe diagnostic and no provider payload: %s", raw)
				}
			})
		}
	}
}
