package client

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/pranavb1234/beacon/internal/pb"
)

// PublisherConfig configures the failover-aware publisher SDK.
type PublisherConfig struct {
	// Brokers is the list of broker addresses to connect to (will rotate on failure).
	Brokers []string

	// ClientID uniquely identifies this publisher instance for idempotency deduplication.
	ClientID string

	// InitialBackoff is the starting delay before retrying a failed publish.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between retries.
	MaxBackoff time.Duration

	// Jitter enables randomized backoff jitter to prevent thundering herds.
	Jitter bool

	// MaxRetries is the maximum number of failover attempts before giving up (0 = default 5).
	MaxRetries int
}

// Publisher is a client-side pub/sub producer that automatically fails over across broker
// nodes and provides idempotent delivery using client sequence numbers.
type Publisher struct {
	cfg     PublisherConfig
	seq     uint64
	connIdx int
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]pb.BrokerClient
	dialer  func(ctx context.Context, addr string) (net.Conn, error)
	rng     *rand.Rand
}

// PublisherOption configures Publisher.
type PublisherOption func(*Publisher)

// WithPublisherDialer sets a custom dialer (e.g., bufconn).
func WithPublisherDialer(d func(ctx context.Context, addr string) (net.Conn, error)) PublisherOption {
	return func(p *Publisher) {
		p.dialer = d
	}
}

// NewPublisher creates an idempotent, reconnecting publisher client.
func NewPublisher(cfg PublisherConfig, opts ...PublisherOption) *Publisher {
	if cfg.ClientID == "" {
		cfg.ClientID = fmt.Sprintf("pub-%d-%d", time.Now().UnixNano(), rand.Int63())
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 20 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 1 * time.Second
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}

	p := &Publisher{
		cfg:     cfg,
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]pb.BrokerClient),
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

// Close closes all open gRPC client connections.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.conns {
		_ = conn.Close()
	}
	p.conns = make(map[string]*grpc.ClientConn)
	p.clients = make(map[string]pb.BrokerClient)
	return nil
}

// Publish sends a payload to topic. If the broker is unavailable or not the owner,
// it retries against another broker with the same client_seq so the broker drops retried duplicates.
func (p *Publisher) Publish(ctx context.Context, topic string, payload []byte) (uint64, error) {
	clientSeq := atomic.AddUint64(&p.seq, 1)

	backoff := p.cfg.InitialBackoff
	maxRetries := p.cfg.MaxRetries

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		client, addr, err := p.getClient()
		if err != nil {
			lastErr = err
			p.rotateBroker()
			continue
		}

		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		resp, err := client.Publish(callCtx, &pb.PublishRequest{
			Topic:     topic,
			Payload:   payload,
			ClientId:  p.cfg.ClientID,
			ClientSeq: clientSeq,
		})
		cancel()

		if err == nil {
			return resp.GetSequence(), nil
		}

		lastErr = err

		// Check if error is transient / failover related
		st, ok := status.FromError(err)
		isFailoverErr := !ok || st.Code() == codes.Unavailable ||
			st.Code() == codes.FailedPrecondition ||
			st.Code() == codes.DeadlineExceeded

		if isFailoverErr {
			p.rotateBroker()

			// Apply backoff with optional jitter
			sleepDuration := backoff
			if p.cfg.Jitter {
				p.mu.Lock()
				jitterFactor := 0.5 + p.rng.Float64()*0.5
				p.mu.Unlock()
				sleepDuration = time.Duration(float64(sleepDuration) * jitterFactor)
			}

			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(sleepDuration):
				backoff *= 2
				if backoff > p.cfg.MaxBackoff {
					backoff = p.cfg.MaxBackoff
				}
			}
		} else {
			// Non-retriable error
			return 0, fmt.Errorf("publish to %s failed: %w", addr, err)
		}
	}

	return 0, fmt.Errorf("publish failed after %d retries, last error: %w", maxRetries, lastErr)
}

func (p *Publisher) getClient() (pb.BrokerClient, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.cfg.Brokers) == 0 {
		return nil, "", fmt.Errorf("no broker addresses configured")
	}

	addr := p.cfg.Brokers[p.connIdx%len(p.cfg.Brokers)]
	if c, ok := p.clients[addr]; ok {
		return c, addr, nil
	}

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if p.dialer != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(p.dialer))
	}

	target := addr
	if p.dialer != nil {
		target = "passthrough:///" + addr
	}

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, addr, fmt.Errorf("dial %s: %w", addr, err)
	}

	client := pb.NewBrokerClient(conn)
	p.conns[addr] = conn
	p.clients[addr] = client

	return client, addr, nil
}

func (p *Publisher) rotateBroker() {
	p.mu.Lock()
	p.connIdx++
	p.mu.Unlock()
}
