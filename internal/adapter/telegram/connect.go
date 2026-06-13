package telegram

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/store/userchannels"
	"github.com/redis/go-redis/v9"
)

const (
	connectTTL = 5 * time.Minute

	// telegramDeepLink is the URL the user clicks to open the Telegram app and
	// start a conversation with the bot. Outbound — goes to the user's browser.
	telegramDeepLink = "https://t.me/%s?start=%s" // args: botUsername, token
)

type connectHandler struct {
	bot      *Bot
	rdb      *redis.Client
	channels *userchannels.Repo
	secret   string
}

func connectKey(token string) string     { return "synapse:tg:connect:" + token }
func connectDoneKey(token string) string { return "synapse:tg:connect:done:" + token }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func requireUser(w http.ResponseWriter, r *http.Request) (string, bool) {
	uid := middleware.UserFromContext(r.Context())
	if uid == "" {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return "", false
	}
	return uid, true
}

// MountRoutes registers the Telegram connect flow under the given root router.
// It is a no-op if the adapter has no bot configured.
func (a *Adapter) MountRoutes(r chi.Router, userMW func(http.Handler) http.Handler, rdb *redis.Client, channels *userchannels.Repo) {
	if a.bot == nil {
		return
	}
	h := &connectHandler{bot: a.bot, rdb: rdb, channels: channels, secret: a.secret}
	r.With(userMW).Get("/v1/me/channels/telegram/connect", h.initiate)
	r.With(userMW).Get("/v1/me/channels/telegram/connect/{token}", h.status)
	// Inbound: Telegram calls this URL to deliver bot updates to us.
	r.Post("/internal/telegram/webhook", h.webhook)
}

// GET /v1/me/channels/telegram/connect
// Generates a one-time token, stores it in Redis, and returns a deep link the
// user clicks to open Telegram and start the connect flow with the bot.
func (h *connectHandler) initiate(w http.ResponseWriter, req *http.Request) {
	uid, ok := requireUser(w, req)
	if !ok {
		return
	}

	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		writeError(w, http.StatusInternalServerError, "token generation failed")
		return
	}
	token := hex.EncodeToString(raw)

	ctx := req.Context()
	if err := h.rdb.Set(ctx, connectKey(token), uid, connectTTL).Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "token storage failed")
		return
	}

	username, err := h.bot.Username(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "telegram bot unavailable")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"url":   fmt.Sprintf(telegramDeepLink, username, token),
		"token": token,
	})
}

// GET /v1/me/channels/telegram/connect/{token}
// Polls whether the user has completed the connect flow. Returns "pending" until
// the bot receives the /start message, then "connected" with the new channel ID.
func (h *connectHandler) status(w http.ResponseWriter, req *http.Request) {
	uid, ok := requireUser(w, req)
	if !ok {
		return
	}

	token := chi.URLParam(req, "token")
	ctx := req.Context()

	if owner, err := h.rdb.Get(ctx, connectKey(token)).Result(); err == nil && owner != uid {
		writeError(w, http.StatusForbidden, "token does not belong to this user")
		return
	}

	channelID, err := h.rdb.Get(ctx, connectDoneKey(token)).Result()
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "pending"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "connected",
		"channel_id": channelID,
	})
}

// POST /internal/telegram/webhook
// Inbound handler: Telegram calls this with bot updates. Verifies the secret
// token, then dispatches /start messages to handleConnect.
func (h *connectHandler) webhook(w http.ResponseWriter, req *http.Request) {
	if h.secret == "" || req.Header.Get("X-Telegram-Bot-Api-Secret-Token") != h.secret {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	var update struct {
		Message *struct {
			Text string `json:"text"`
			Chat struct {
				ID int64 `json:"id"`
			} `json:"chat"`
			From struct {
				FirstName string `json:"first_name"`
			} `json:"from"`
		} `json:"message"`
	}
	if err := json.NewDecoder(req.Body).Decode(&update); err != nil || update.Message == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	text := strings.TrimSpace(update.Message.Text)
	chatID := strconv.FormatInt(update.Message.Chat.ID, 10)

	if !strings.HasPrefix(text, "/start") {
		w.WriteHeader(http.StatusOK)
		return
	}

	ctx := req.Context()
	token := strings.TrimSpace(strings.TrimPrefix(text, "/start"))
	if token == "" {
		_ = h.bot.Send(ctx, chatID, "Open the Synapse UI and click \"Connect Telegram\" to get your personal link.")
		w.WriteHeader(http.StatusOK)
		return
	}

	h.handleConnect(ctx, chatID, token, update.Message.From.FirstName)
	w.WriteHeader(http.StatusOK)
}

func (h *connectHandler) handleConnect(ctx context.Context, chatID, token, firstName string) {
	userID, err := h.rdb.GetDel(ctx, connectKey(token)).Result()
	if err != nil {
		_ = h.bot.Send(ctx, chatID, "This link has expired or is invalid. Please generate a new one from the Synapse UI.")
		return
	}

	label := "Telegram"
	if firstName != "" {
		label = "Telegram (" + firstName + ")"
	}
	config, _ := json.Marshal(map[string]string{"chat_id": chatID})

	channelID, err := h.channels.Insert(ctx, userchannels.UserChannel{
		UserID:      userID,
		ChannelType: "telegram",
		Label:       label,
		Config:      config,
	})
	if err != nil {
		_ = h.bot.Send(ctx, chatID, "Something went wrong setting up your channel. Please try again.")
		return
	}

	h.rdb.Set(ctx, connectDoneKey(token), strconv.FormatInt(channelID, 10), connectTTL)
	_ = h.bot.Send(ctx, chatID, "✓ Connected! You'll receive Synapse notifications here.")
}
