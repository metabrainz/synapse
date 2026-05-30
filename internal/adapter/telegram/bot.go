package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Bot is a thin Telegram Bot API client. It is safe for concurrent use.
type Bot struct {
	token  string
	client *http.Client

	mu       sync.RWMutex
	username string // cached from GetMe; empty until first call
}

func NewBot(token string) *Bot {
	return &Bot{
		token:  token,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Username returns the bot's @username, fetching it from Telegram on first call.
func (b *Bot) Username(ctx context.Context) (string, error) {
	b.mu.RLock()
	u := b.username
	b.mu.RUnlock()
	if u != "" {
		return u, nil
	}

	raw, err := b.call(ctx, "getMe", nil)
	if err != nil {
		return "", err
	}
	var me struct {
		Username string `json:"username"`
	}
	if err := json.Unmarshal(raw, &me); err != nil {
		return "", fmt.Errorf("telegram: parse getMe: %w", err)
	}

	b.mu.Lock()
	b.username = me.Username
	b.mu.Unlock()
	return me.Username, nil
}

// SetWebhook registers url as the Telegram webhook endpoint.
// secret is forwarded to Synapse as X-Telegram-Bot-Api-Secret-Token on every update.
func (b *Bot) SetWebhook(ctx context.Context, url, secret string) error {
	_, err := b.call(ctx, "setWebhook", map[string]any{
		"url":             url,
		"secret_token":    secret,
		"allowed_updates": []string{"message"},
	})
	return err
}

// Send sends a plain-text message to chatID.
func (b *Bot) Send(ctx context.Context, chatID, text string) error {
	_, err := b.call(ctx, "sendMessage", map[string]any{
		"chat_id": chatID,
		"text":    text,
	})
	return err
}

func (b *Bot) call(ctx context.Context, method string, body any) (json.RawMessage, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(data)
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/%s", b.token, method)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reqBody)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("telegram: %s: %w", method, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var result struct {
		OK          bool            `json:"ok"`
		Result      json.RawMessage `json:"result"`
		Description string          `json:"description"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("telegram: %s: unexpected response (HTTP %d)", method, resp.StatusCode)
	}
	if !result.OK {
		return nil, fmt.Errorf("telegram: %s: %s", method, result.Description)
	}
	return result.Result, nil
}
