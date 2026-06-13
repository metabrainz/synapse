package webhook_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/adapter/webhook"
	"github.com/metabrainz/synapse/internal/fanout"
)

func TestValidateConfig(t *testing.T) {
	a := webhook.New(true)

	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{"missing url key", `{}`, true},
		{"empty url", `{"url":""}`, true},
		{"invalid json", `not-json`, true},
		{"bare hostname no scheme", `{"url":"example.com/hook"}`, true},
		{"no host", `{"url":"http://"}`, true},
		{"ftp scheme", `{"url":"ftp://example.com/hook"}`, true},
		{"file scheme", `{"url":"file:///etc/passwd"}`, true},
		{"data scheme", `{"url":"data:text/plain,hello"}`, true},
		{"valid http", `{"url":"http://example.com/hook"}`, false},
		{"valid https", `{"url":"https://example.com/hook"}`, false},
		{"valid https with path and query", `{"url":"https://example.com/webhook?token=abc"}`, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := a.ValidateConfig(json.RawMessage(tc.config))
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateConfig(%s) err=%v, wantErr=%v", tc.config, err, tc.wantErr)
			}
		})
	}
}

func TestSSRFProtection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	msg := fanout.WorkerMessage{
		DeliveryID:    1,
		ChannelConfig: json.RawMessage(`{"url":"` + srv.URL + `"}`),
		EventType:     "test.event",
		TenantID:      "test-tenant",
		UserID:        "user-1",
		Payload:       json.RawMessage(`{}`),
		CreatedAt:     time.Now(),
	}

	t.Run("blocks localhost when allowPrivateURLs=false", func(t *testing.T) {
		a := webhook.New(false)
		err := a.Deliver(context.Background(), msg)
		if err == nil {
			t.Fatal("expected SSRF block, got nil error")
		}
	})

	t.Run("allows localhost when allowPrivateURLs=true", func(t *testing.T) {
		a := webhook.New(true)
		err := a.Deliver(context.Background(), msg)
		if err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
	})
}

// TestSSRFBlocklist verifies specific IP ranges are rejected.
func TestSSRFBlocklist(t *testing.T) {
	blocked := []string{
		"http://127.0.0.1/hook",
		"http://127.0.0.2/hook",
		"http://10.0.0.1/hook",
		"http://10.255.255.255/hook",
		"http://172.16.0.1/hook",
		"http://172.31.255.255/hook",
		"http://192.168.1.1/hook",
		"http://169.254.169.254/hook", // AWS/GCP metadata
		"http://169.254.169.254/latest/meta-data/iam/security-credentials/",
	}

	a := webhook.New(false)
	for _, url := range blocked {
		t.Run(url, func(t *testing.T) {
			msg := fanout.WorkerMessage{
				DeliveryID:    1,
				ChannelConfig: json.RawMessage(`{"url":"` + url + `"}`),
				Payload:       json.RawMessage(`{}`),
				CreatedAt:     time.Now(),
			}
			err := a.Deliver(context.Background(), msg)
			if err == nil {
				t.Errorf("expected SSRF block for %s, got nil error", url)
			}
		})
	}
}
