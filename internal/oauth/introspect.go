package oauth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrInactive is returned when the token is expired, revoked, or not recognised by MB.
var ErrInactive = errors.New("oauth: token inactive")

const defaultTokenTTL = 15 * time.Minute

// Claims holds the validated identity extracted from a MetaBrainz OAuth token.
type Claims struct {
	ID       string
	Username string
}

// Introspector validates a Bearer token and returns the caller's identity.
type Introspector interface {
	Introspect(ctx context.Context, token string) (Claims, error)
}

type introspectResponse struct {
	Active    bool   `json:"active"`
	Sub       string `json:"sub"`
	Username  string `json:"username"`
	ExpiresAt int64  `json:"expires_at"`
}

// MBIntrospector calls the MetaBrainz OAuth2 introspection endpoint.
type MBIntrospector struct {
	clientID     string
	clientSecret string
	endpoint     string
	rdb          *redis.Client
	httpClient   *http.Client
}

// NewMBIntrospector constructs a MBIntrospector.
func NewMBIntrospector(clientID, clientSecret, endpoint string, rdb *redis.Client) *MBIntrospector {
	return &MBIntrospector{
		clientID:     clientID,
		clientSecret: clientSecret,
		endpoint:     endpoint,
		rdb:          rdb,
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}
}

func cacheKey(token string) string {
	h := sha256.Sum256([]byte(token))
	return "oauth:token:" + hex.EncodeToString(h[:])
}

// Introspect validates the given token against the MetaBrainz introspection endpoint.
// It returns the caller's Claims on success, ErrInactive if the token is not valid,
// or a wrapped error for network/protocol failures.
func (m *MBIntrospector) Introspect(ctx context.Context, token string) (Claims, error) {
	if m.rdb != nil {
		if cached, err := m.rdb.Get(ctx, cacheKey(token)).Result(); err == nil {
			if cached == "" {
				return Claims{}, ErrInactive
			}
			parts := strings.SplitN(cached, ":", 2)
			c := Claims{ID: parts[0]}
			if len(parts) == 2 {
				c.Username = parts[1]
			}
			return c, nil
		} else if !errors.Is(err, redis.Nil) {
			slog.Warn("oauth: redis cache read failed, falling through to introspection", "err", err)
		}
	}

	form := url.Values{
		"client_id":       {m.clientID},
		"client_secret":   {m.clientSecret},
		"token":           {token},
		"token_type_hint": {"access_token"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.endpoint,
		strings.NewReader(form.Encode()))
	if err != nil {
		return Claims{}, fmt.Errorf("oauth: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return Claims{}, fmt.Errorf("oauth: introspect request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Claims{}, fmt.Errorf("oauth: introspect: upstream status %d", resp.StatusCode)
	}

	resp.Body = http.MaxBytesReader(nil, resp.Body, 64*1024)

	var ir introspectResponse
	if err := json.NewDecoder(resp.Body).Decode(&ir); err != nil {
		return Claims{}, fmt.Errorf("oauth: decode introspect response: %w", err)
	}

	// sub="-1" = client-credentials grant (no user); reject for /v1/me.
	if !ir.Active || (ir.ExpiresAt > 0 && time.Now().Unix() > ir.ExpiresAt) ||
		ir.Sub == "" || ir.Sub == "-1" {
		if m.rdb != nil {
			if err := m.rdb.Set(ctx, cacheKey(token), "", 30*time.Second).Err(); err != nil {
				slog.Warn("oauth: redis cache write failed", "err", err)
			}
		}
		return Claims{}, ErrInactive
	}

	claims := Claims{
		ID:       ir.Sub,
		Username: ir.Username,
	}

	if m.rdb != nil {
		var ttl time.Duration
		if ir.ExpiresAt > 0 {
			ttl = time.Until(time.Unix(ir.ExpiresAt, 0))
		} else {
			ttl = defaultTokenTTL
		}
		if ttl > 0 {
			if err := m.rdb.Set(ctx, cacheKey(token), claims.ID+":"+claims.Username, ttl).Err(); err != nil {
				slog.Warn("oauth: redis cache write failed", "err", err)
			}
		}
	}
	return claims, nil
}
