package live

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const TicketTTL = 30 * time.Second

var ErrInvalidTicket = errors.New("live: invalid stream ticket")

type Tickets struct {
	rdb *redis.Client
}

func NewTickets(rdb *redis.Client) *Tickets {
	return &Tickets{rdb: rdb}
}

func ticketKey(ticket string) string {
	return "synapse:stream-ticket:" + ticket
}

func (t *Tickets) Mint(ctx context.Context, userID string) (string, error) {
	ticket := rand.Text()
	if err := t.rdb.Set(ctx, ticketKey(ticket), userID, TicketTTL).Err(); err != nil {
		return "", fmt.Errorf("store stream ticket: %w", err)
	}
	return ticket, nil
}

// Redeem consumes the ticket; a second call with the same ticket fails.
func (t *Tickets) Redeem(ctx context.Context, ticket string) (string, error) {
	if ticket == "" {
		return "", ErrInvalidTicket
	}
	userID, err := t.rdb.GetDel(ctx, ticketKey(ticket)).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrInvalidTicket
	}
	if err != nil {
		return "", fmt.Errorf("redeem stream ticket: %w", err)
	}
	return userID, nil
}
