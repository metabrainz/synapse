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

const connectTTL = 5 * time.Minute

type connectHandler struct {
	bot      *Bot
	rdb      *redis.Client
	channels *userchannels.Repo
	secret   string
}

// A connect key is used to store the token and user ID during the connect flow.
func connectKey(token string) string     { return "synapse:tg:connect:" + token }

// A done key is used to store the channel ID after the connect flow is complete.
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
	r.Post("/internal/telegram/webhook", h.webhook)
}

// GET /v1/me/channels/telegram/connect
/*
	Initiates the connect flow by generating a random token and storing it in Redis.
*/
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
		"url":   fmt.Sprintf("https://t.me/%s?start=%s", username, token),
		"token": token,
	})
}

// GET /v1/me/channels/telegram/connect/{token}
/*
	Checks the status of the connect flow by looking up the token in Redis.
*/
func (h *connectHandler) status(w http.ResponseWriter, req *http.Request) {
	uid, ok := requireUser(w, req)
	if !ok {
		return
	}

	// Get the token from the URL path.
	token := chi.URLParam(req, "token")
	ctx := req.Context()

	// Check if the token belongs to the user.
	if owner, err := h.rdb.Get(ctx, connectKey(token)).Result(); err == nil && owner != uid {
		writeError(w, http.StatusForbidden, "token does not belong to this user")
		return
	}

	// Check if the connect flow is complete.
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
/*
	Handles the Telegram webhook by verifying the secret token and parsing the update.
*/
func (h *connectHandler) webhook(w http.ResponseWriter, req *http.Request) {
	if h.secret == "" || req.Header.Get("X-Telegram-Bot-Api-Secret-Token") != h.secret {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Parse the update from the request body.
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

	// Check if the message is a start command.
	if !strings.HasPrefix(text, "/start") {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Get the token from the start command.
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
