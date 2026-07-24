package email

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSendSingle_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/send_single" {
			t.Fatalf("expected /send_single, got %s", r.URL.Path)
		}

		body, _ := io.ReadAll(r.Body)
		var req SendRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("unmarshal request: %v", err)
		}
		if req.TemplateID != "basic" {
			t.Errorf("expected template_id=basic, got %s", req.TemplateID)
		}
		if req.To != "user@example.com" {
			t.Errorf("expected to=user@example.com, got %s", req.To)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"t":"Success","c":{"code":250,"message":"OK\n"}}`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	err := client.SendSingle(context.Background(), SendRequest{
		TemplateID: "basic",
		From:       "noreply@test.org",
		To:         "user@example.com",
		Lang:       "en",
		Params:     json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestSendSingle_404_Permanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Failed to parse MJML: template-not-found"))
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	err := client.SendSingle(context.Background(), SendRequest{
		TemplateID: "nonexistent",
		From:       "noreply@test.org",
		To:         "user@example.com",
		Params:     json.RawMessage(`{}`),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	me, ok := err.(*MailError)
	if !ok {
		t.Fatalf("expected *MailError, got %T", err)
	}
	if me.StatusCode != 404 {
		t.Errorf("expected status 404, got %d", me.StatusCode)
	}
	if !me.Permanent() {
		t.Error("expected permanent error for 404")
	}
}

func TestSendSingle_500_Transient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to send mail: SMTP relay down"))
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	err := client.SendSingle(context.Background(), SendRequest{
		TemplateID: "basic",
		From:       "noreply@test.org",
		To:         "user@example.com",
		Params:     json.RawMessage(`{}`),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	me, ok := err.(*MailError)
	if !ok {
		t.Fatalf("expected *MailError, got %T", err)
	}
	if me.StatusCode != 500 {
		t.Errorf("expected status 500, got %d", me.StatusCode)
	}
	if me.Permanent() {
		t.Error("expected transient error for 500")
	}
}

func TestValidateConfig(t *testing.T) {
	a := New("http://localhost:3000", nil, nil)

	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{"valid", `{"to":"user@example.com"}`, false},
		{"missing to", `{}`, true},
		{"no @", `{"to":"notanemail"}`, true},
		{"bad json", `{broken`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := a.ValidateConfig(json.RawMessage(tt.config))
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
