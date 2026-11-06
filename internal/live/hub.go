package live

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const streamBuffer = 8

var ErrHubClosed = errors.New("live: hub closed")

type userStreams struct {
	chans map[chan Nudge]struct{}
	ready chan struct{} // closed and set to nil once Redis confirms the SUBSCRIBE
}

// Hub fans nudges from one shared Redis PubSub connection out to this
// instance's open streams. Maps are keyed by Redis channel name.
type Hub struct {
	rdb *redis.Client

	mu      sync.Mutex
	ps      *redis.PubSub
	streams map[string]*userStreams
	// SUBSCRIBEs sent whose confirmation hasn't been read yet, so a late reply
	// from an earlier subscribe can't mark a newer registration ready.
	unconfirmed map[string]int
	closed      bool
}

func NewHub(rdb *redis.Client) *Hub {
	return &Hub{
		rdb:         rdb,
		ps:          rdb.Subscribe(context.Background()),
		streams:     map[string]*userStreams{},
		unconfirmed: map[string]int{},
	}
}

// Run receives from Redis until ctx is done, then closes the hub.
func (h *Hub) Run(ctx context.Context) {
	// Receive has no read deadline and ignores cancellation; closing the PubSub unblocks it.
	context.AfterFunc(ctx, h.Close)
	defer h.Close()

	h.mu.Lock()
	ps := h.ps
	h.mu.Unlock()

	for {
		msg, err := ps.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			h.mu.Lock()
			if h.closed {
				h.mu.Unlock()
				return
			}
			slog.Warn("live: hub subscription lost", "err", err)
			h.dropAll()
			// A fresh PubSub forgets the old channels; go-redis would otherwise
			// resubscribe them on reconnect and their replies would confuse confirmation.
			_ = ps.Close()
			h.ps = h.rdb.Subscribe(context.Background())
			ps = h.ps
			h.mu.Unlock()

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		switch m := msg.(type) {
		case *redis.Subscription:
			if m.Kind == "subscribe" {
				h.confirm(m.Channel)
			}
		case *redis.Message:
			var n Nudge
			if err := json.Unmarshal([]byte(m.Payload), &n); err != nil {
				slog.Warn("live: bad nudge", "channel", m.Channel, "err", err)
				continue
			}
			h.fanOut(m.Channel, n)
		}
	}
}

// Register returns once Redis has confirmed the user's subscription, so a
// nudge published after it returns is delivered.
func (h *Hub) Register(ctx context.Context, userID string) (<-chan Nudge, func(), error) {
	channel := Channel(userID)

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, nil, ErrHubClosed
	}
	s := h.streams[channel]
	if s == nil {
		// The connection is shared: a request deadline here would become a socket
		// write deadline, and a write error resets every stream on the instance.
		if err := h.ps.Subscribe(context.WithoutCancel(ctx), channel); err != nil {
			h.mu.Unlock()
			return nil, nil, err
		}
		s = &userStreams{chans: map[chan Nudge]struct{}{}, ready: make(chan struct{})}
		h.streams[channel] = s
		h.unconfirmed[channel]++
	}
	ch := make(chan Nudge, streamBuffer)
	s.chans[ch] = struct{}{}
	ready := s.ready
	h.mu.Unlock()

	unregister := func() { h.unregister(channel, ch) }
	if ready == nil {
		return ch, unregister, nil
	}
	select {
	case <-ready:
		return ch, unregister, nil
	case <-ctx.Done():
		unregister()
		return nil, nil, ctx.Err()
	}
}

// Close closes every stream channel; later Registers return ErrHubClosed.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	h.dropAll()
	_ = h.ps.Close()
}

func (h *Hub) unregister(channel string, ch chan Nudge) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.streams[channel]
	if s == nil {
		return
	}
	if _, ok := s.chans[ch]; !ok {
		return
	}
	delete(s.chans, ch)
	close(ch)
	if len(s.chans) == 0 {
		delete(h.streams, channel)
		_ = h.ps.Unsubscribe(context.Background(), channel)
	}
}

func (h *Hub) confirm(channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.unconfirmed[channel]--
	if h.unconfirmed[channel] > 0 {
		return
	}
	delete(h.unconfirmed, channel)
	if s := h.streams[channel]; s != nil && s.ready != nil {
		close(s.ready)
		s.ready = nil
	}
}

func (h *Hub) fanOut(channel string, n Nudge) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.streams[channel]
	if s == nil {
		return
	}
	for ch := range s.chans {
		select {
		case ch <- n:
		default:
		}
	}
}

// dropAll closes every stream and releases pending Registers, which then see a closed channel.
func (h *Hub) dropAll() {
	for _, s := range h.streams {
		for ch := range s.chans {
			close(ch)
		}
		if s.ready != nil {
			close(s.ready)
		}
	}
	clear(h.streams)
	clear(h.unconfirmed)
}
