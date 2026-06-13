package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/metabrainz/synapse/internal/fanout"
)

type channelConfig struct {
	URL string `json:"url"`
}

// deliveryPayload is what the receiving webhook endpoint sees.
type deliveryPayload struct {
	EventType  string          `json:"event_type"`
	TenantID   string          `json:"tenant_id"`
	UserID     string          `json:"user_id"`
	Payload    json.RawMessage `json:"payload"`
	DeliveryID int64           `json:"delivery_id"`
	CreatedAt  time.Time       `json:"created_at"`
}

type Adapter struct {
	client *http.Client
}

// blockedCIDRs is the set of IP ranges the webhook transport will refuse to dial.
// Evaluated after DNS resolution so DNS rebinding attacks are also caught.
var blockedCIDRs = func() []*net.IPNet {
	cidrs := []string{
		"127.0.0.0/8",    // loopback
		"::1/128",        // IPv6 loopback
		"10.0.0.0/8",     // RFC1918
		"172.16.0.0/12",  // RFC1918
		"192.168.0.0/16", // RFC1918
		"169.254.0.0/16", // link-local — AWS/GCP metadata endpoint lives here
		"fe80::/10",      // IPv6 link-local
		"fc00::/7",       // IPv6 unique-local
		"0.0.0.0/8",      // unspecified
	}
	nets := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, _ := net.ParseCIDR(c)
		nets = append(nets, n)
	}
	return nets
}()

// safeDialContext resolves the hostname and refuses to connect if any resolved
// IP falls in a blocked range. This runs at dial time (post-DNS), which means
// it catches DNS rebinding — a public hostname that flips to a private IP.
func safeDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	// If host is already an IP literal, skip DNS and check it directly.
	var ips []string
	if net.ParseIP(host) != nil {
		ips = []string{host}
	} else {
		ips, err = net.DefaultResolver.LookupHost(ctx, host)
		if err != nil {
			return nil, err
		}
	}

	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		for _, blocked := range blockedCIDRs {
			if blocked.Contains(ip) {
				return nil, fmt.Errorf("webhook: host %s resolves to blocked IP %s", host, ipStr)
			}
		}
	}
	var d net.Dialer
	return d.DialContext(ctx, network, net.JoinHostPort(ips[0], port))
}

// New creates a webhook adapter.
// allowPrivateURLs disables the SSRF blocklist — set true only in development.
func New(allowPrivateURLs bool) *Adapter {
	transport := &http.Transport{MaxIdleConnsPerHost: 100}
	if !allowPrivateURLs {
		transport.DialContext = safeDialContext
	}
	return &Adapter{client: &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}}
}

func (a *Adapter) MaxAttempts() int { return 5 }

// ValidateConfig checks that the webhook config has a non-empty, well-formed URL
// with an http or https scheme.
func (a *Adapter) ValidateConfig(config json.RawMessage) error {
	var cfg channelConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	if cfg.URL == "" {
		return fmt.Errorf("url is required")
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("url scheme must be http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("url must have a host")
	}
	return nil
}

// Deliver POSTs the event payload to the webhook URL in the channel config.
// X-Synapse-* headers let the receiver validate delivery metadata without
// parsing the body (useful for HMAC verification or logging).
func (a *Adapter) Deliver(ctx context.Context, msg fanout.WorkerMessage) error {
	var cfg channelConfig
	if err := json.Unmarshal(msg.ChannelConfig, &cfg); err != nil {
		return fmt.Errorf("invalid channel config: %w", err)
	}

	body, err := json.Marshal(deliveryPayload{
		EventType:  msg.EventType,
		TenantID:   msg.TenantID,
		UserID:     msg.UserID,
		Payload:    msg.Payload,
		DeliveryID: msg.DeliveryID,
		CreatedAt:  msg.CreatedAt,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Synapse-Event-Type", msg.EventType)
	req.Header.Set("X-Synapse-Tenant-ID", msg.TenantID)
	req.Header.Set("X-Synapse-Delivery-ID", strconv.FormatInt(msg.DeliveryID, 10))

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Drain body so the connection can be reused by the pool.
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}
