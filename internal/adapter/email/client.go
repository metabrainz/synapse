package email

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type SendRequest struct {
	TemplateID string          `json:"template_id"`
	From       string          `json:"from"`
	To         string          `json:"to"`
	Lang       string          `json:"lang,omitempty"`
	Params     json.RawMessage `json:"params"`
}

// MailError is returned for non-OK responses from mb-mail-service.
type MailError struct {
	StatusCode int
	Message    string
	permanent  bool
}

func (e *MailError) Error() string {
	return fmt.Sprintf("email: mb-mail-service HTTP %d: %s", e.StatusCode, e.Message)
}

// Permanent returns true when retrying cannot fix the error (4xx = config/data bug).
func (e *MailError) Permanent() bool { return e.permanent }

type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *Client) SendSingle(ctx context.Context, req SendRequest) error {
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/send_single", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return fmt.Errorf("email: send_single: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	return &MailError{
		StatusCode: resp.StatusCode,
		Message:    string(raw),
		permanent:  resp.StatusCode >= 400 && resp.StatusCode < 500,
	}
}
