package agentsessionsd

import (
	"strings"
	"testing"
)

func TestBindingInvalidEnvironmentCredentialRefusesStartup(t *testing.T) {
	const name = "BINDING_INVALID_HEADER_KEY"
	for _, value := range []string{"private\r\nmarker", "private\x7fmarker"} {
		t.Setenv(name, value)
		_, err := loadModelBindings(bindingFile(t, bindingJSON("https://example.test/model", `,"credential":{"type":"env","name":"`+name+`"}`)), nil)
		if err == nil {
			t.Fatal("invalid known environment credential accepted at startup")
		}
		if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), name) {
			t.Fatal("invalid credential exposed in startup error")
		}
	}
}
