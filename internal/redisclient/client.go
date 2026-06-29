// Package redisclient is the ONLY way any mini-k8s component touches Redis.
// It owns the connection pool and exposes typed helpers for hash, set, list,
// key, stream, Pub/Sub, and Lua script operations. Centralizes timeouts,
// retries, and context propagation.
package redisclient

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultTimeout is used for operations when no deadline is set on the context.
const DefaultTimeout = 5 * time.Second

// Client wraps a Redis connection pool and provides typed helpers.
type Client struct {
	rdb *redis.Client
}

// StreamEntry represents a single entry read from a Redis stream.
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

// Subscription wraps a Redis Pub/Sub subscription.
type Subscription struct {
	pubsub *redis.PubSub
}

// New creates a new Redis client, pings to verify reachability, and returns
// the client. It fails fast if Redis is unavailable.
func New(addr string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  DefaultTimeout,
		ReadTimeout:  DefaultTimeout,
		WriteTimeout: DefaultTimeout,
		PoolSize:     10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("redis unreachable at %s: %w", addr, err)
	}

	return &Client{rdb: rdb}, nil
}

// Ping checks Redis reachability.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Close shuts down the connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// ─── Hash helpers ───────────────────────────────────────────────────────────

// HashSet writes fields to a Redis hash.
func (c *Client) HashSet(ctx context.Context, key string, fields map[string]interface{}) error {
	return c.rdb.HSet(ctx, key, fields).Err()
}

// HashGet reads all fields of a Redis hash.
func (c *Client) HashGet(ctx context.Context, key string) (map[string]string, error) {
	return c.rdb.HGetAll(ctx, key).Result()
}

// HashGetField reads a single field from a Redis hash.
func (c *Client) HashGetField(ctx context.Context, key, field string) (string, error) {
	return c.rdb.HGet(ctx, key, field).Result()
}

// ─── Set helpers ────────────────────────────────────────────────────────────

// SetAdd adds a member to a Redis set.
func (c *Client) SetAdd(ctx context.Context, key, member string) error {
	return c.rdb.SAdd(ctx, key, member).Err()
}

// SetMembers returns all members of a Redis set.
func (c *Client) SetMembers(ctx context.Context, key string) ([]string, error) {
	return c.rdb.SMembers(ctx, key).Result()
}

// SetRemove removes a member from a Redis set.
func (c *Client) SetRemove(ctx context.Context, key, member string) error {
	return c.rdb.SRem(ctx, key, member).Err()
}

// ─── List helpers ───────────────────────────────────────────────────────────

// ListPush appends a value to the right end of a list.
func (c *Client) ListPush(ctx context.Context, key, value string) error {
	return c.rdb.RPush(ctx, key, value).Err()
}

// ListTrim trims a list to the specified range (0-indexed, inclusive).
func (c *Client) ListTrim(ctx context.Context, key string, start, stop int64) error {
	return c.rdb.LTrim(ctx, key, start, stop).Err()
}

// ListRange returns elements from a list within the specified range.
func (c *Client) ListRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.rdb.LRange(ctx, key, start, stop).Result()
}

// BlockingPop removes and returns an element from the left end of a list,
// blocking until one is available or the timeout elapses.
func (c *Client) BlockingPop(ctx context.Context, key string, timeout time.Duration) (string, error) {
	result, err := c.rdb.BLPop(ctx, timeout, key).Result()
	if err != nil {
		return "", err
	}
	// BLPop returns [key, value]
	if len(result) < 2 {
		return "", fmt.Errorf("unexpected BLPop result length: %d", len(result))
	}
	return result[1], nil
}

// ─── Key helpers ────────────────────────────────────────────────────────────

// SetKey sets a string key with an optional TTL (0 means no expiry).
func (c *Client) SetKey(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.rdb.Set(ctx, key, value, ttl).Err()
}

// GetKey gets the value of a string key.
func (c *Client) GetKey(ctx context.Context, key string) (string, error) {
	return c.rdb.Get(ctx, key).Result()
}

// DeleteKey deletes one or more keys.
func (c *Client) DeleteKey(ctx context.Context, keys ...string) error {
	return c.rdb.Del(ctx, keys...).Err()
}

// Exists checks whether a key exists (returns true if it does).
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	n, err := c.rdb.Exists(ctx, key).Result()
	return n > 0, err
}

// Expire sets a TTL on an existing key.
func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.rdb.Expire(ctx, key, ttl).Err()
}

// ─── Stream helpers ─────────────────────────────────────────────────────────

// StreamAppend appends an entry to a Redis stream using auto-generated IDs.
func (c *Client) StreamAppend(ctx context.Context, key string, fields map[string]interface{}) error {
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: fields,
	}).Err()
}

// StreamRead reads entries from a Redis stream starting after lastID.
// Use "0" to read from the beginning. Returns up to count entries.
func (c *Client) StreamRead(ctx context.Context, key, lastID string, count int64) ([]StreamEntry, error) {
	results, err := c.rdb.XRangeN(ctx, key, lastID, "+", count).Result()
	if err != nil {
		return nil, err
	}

	entries := make([]StreamEntry, 0, len(results))
	for _, msg := range results {
		fields := make(map[string]string, len(msg.Values))
		for k, v := range msg.Values {
			fields[k] = fmt.Sprintf("%v", v)
		}
		entries = append(entries, StreamEntry{
			ID:     msg.ID,
			Fields: fields,
		})
	}
	return entries, nil
}

// StreamTrim caps a stream to approximately maxLen entries.
func (c *Client) StreamTrim(ctx context.Context, key string, maxLen int64) error {
	return c.rdb.XTrimMaxLen(ctx, key, maxLen).Err()
}

// ─── Pub/Sub helpers ────────────────────────────────────────────────────────

// Publish sends a message to a Pub/Sub channel.
func (c *Client) Publish(ctx context.Context, channel, message string) error {
	return c.rdb.Publish(ctx, channel, message).Err()
}

// Subscribe creates a subscription to a Pub/Sub channel.
func (c *Client) Subscribe(ctx context.Context, channel string) (*Subscription, error) {
	pubsub := c.rdb.Subscribe(ctx, channel)
	// Wait for confirmation that subscription is created
	_, err := pubsub.Receive(ctx)
	if err != nil {
		pubsub.Close()
		return nil, fmt.Errorf("subscribe to %s: %w", channel, err)
	}
	return &Subscription{pubsub: pubsub}, nil
}

// Channel returns a Go channel that receives messages from the subscription.
func (s *Subscription) Channel() <-chan *redis.Message {
	return s.pubsub.Channel()
}

// Close unsubscribes and releases resources.
func (s *Subscription) Close() error {
	return s.pubsub.Close()
}

// ─── Lua script helper ─────────────────────────────────────────────────────

// EvalScript evaluates a Lua script against the given keys and arguments.
func (c *Client) EvalScript(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return c.rdb.Eval(ctx, script, keys, args...).Result()
}

// ─── Key scanning helper ───────────────────────────────────────────────────

// ScanKeys finds keys matching a pattern. Used for finding pods by prefix, etc.
func (c *Client) ScanKeys(ctx context.Context, pattern string) ([]string, error) {
	var allKeys []string
	var cursor uint64
	for {
		keys, nextCursor, err := c.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		allKeys = append(allKeys, keys...)
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return allKeys, nil
}
