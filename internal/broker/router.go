// Package broker implements request routing across cluster nodes.
//
// When a client calls Publish or Subscribe on a node that is NOT the topic owner
// (per the consistent hash ring), the Router forwards the call to the owner node
// and streams back the response. If the caller is already the owner, the request
// is handled locally.
//
// Forwarding loop prevention: a hop_count field is attached to every ForwardPublish
// request. If a node receives a request with hop_count > 1 it rejects it with
// codes.ResourceExhausted.
//
// Connection pool: one *grpc.ClientConn per peer node ID, created lazily and
// kept alive for the lifetime of the Router. Closing the Router closes all
// pooled connections.
package broker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/pranavb1234/beacon/internal/cluster"
	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/pb"
)

const (
	// maxHopCount is the maximum number of forwarding hops allowed before a request is rejected.
	maxHopCount = 1

	// defaultForwardTimeout is the default deadline applied to individual forward RPCs.
	defaultForwardTimeout = 5 * time.Second
)

// DialFunc is a pluggable dialer used for testing (e.g., bufconn).
type DialFunc func(ctx context.Context, addr string) (interface{}, error)

// connEntry is a lazily-initialized gRPC connection to a peer node.
type connEntry struct {
	mu     sync.Mutex
	conn   *grpc.ClientConn
	client pb.InternalClient
	broker pb.BrokerClient // for Ack forwarding via the public Broker service
}

// Router routes Publish and Subscribe requests to the topic's owner node
// according to the consistent hash ring, maintaining a connection pool of
// reusable gRPC client connections.
type Router struct {
	// Embed UnimplementedInternalServer so Router satisfies the full pb.InternalServer interface.
	// The Heartbeat RPC is handled by cluster.Membership; Router only handles Forward + ForwardSubscribe.
	pb.UnimplementedInternalServer

	// nodeID is this broker's own identifier.
	nodeID string
	// ring is the shared consistent hash ring (kept up to date by Membership).
	ring *cluster.Ring
	// membership provides peer address lookup.
	membership *cluster.Membership
	// localServer handles requests that are owned by this node.
	localServer *Server
	// logger for routing events.
	logger *slog.Logger

	// cfg holds node address / timeout configuration.
	cfg *config.Config

	// pool is the connection pool, keyed by peer node ID.
	poolMu sync.RWMutex
	pool   map[string]*connEntry

	// customDialer is optional; set during tests to route via bufconn.
	customDialer cluster.DialerFunc
}

// RouterOption configures a Router.
type RouterOption func(*Router)

// WithRouterDialer injects a custom dialer (e.g. bufconn) for unit tests.
func WithRouterDialer(d cluster.DialerFunc) RouterOption {
	return func(r *Router) {
		r.customDialer = d
	}
}

// NewRouter creates a Router wired to the given ring and local server.
func NewRouter(
	nodeID string,
	ring *cluster.Ring,
	membership *cluster.Membership,
	localServer *Server,
	cfg *config.Config,
	logger *slog.Logger,
	opts ...RouterOption,
) *Router {
	if logger == nil {
		logger = slog.Default()
	}
	r := &Router{
		nodeID:      nodeID,
		ring:        ring,
		membership:  membership,
		localServer: localServer,
		cfg:         cfg,
		logger:      logger.With("module", "router", "local_node", nodeID),
		pool:        make(map[string]*connEntry),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Close drains and closes all pooled gRPC connections.
func (r *Router) Close() {
	r.poolMu.Lock()
	defer r.poolMu.Unlock()
	for id, e := range r.pool {
		e.mu.Lock()
		if e.conn != nil {
			_ = e.conn.Close()
			e.conn = nil
		}
		e.mu.Unlock()
		delete(r.pool, id)
	}
}

// OwnerOf returns the node ID that owns the given topic key.
func (r *Router) OwnerOf(topic string) string {
	return r.ring.GetOwner(topic)
}

// ----------------------------------------------------------------------------
// Publish routing
// ----------------------------------------------------------------------------

// RoutePublish routes a Publish RPC to the topic's owner.
// If this node is the owner, it publishes locally.
// Otherwise it forwards to the owner node with hop_count + 1.
func (r *Router) RoutePublish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if req.GetTopic() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	owner := r.ring.GetOwner(req.GetTopic())
	if owner == "" {
		return nil, status.Error(codes.Unavailable, "no node available for topic (ring is empty)")
	}

	if owner == r.nodeID {
		return r.localServer.publishLocal(ctx, req)
	}

	// Forward to owner
	return r.forwardPublish(ctx, owner, req, 0)
}

// forwardPublish sends a ForwardPublish RPC to targetNode.
// It retries once if the ring's owner changes between the first and second attempt.
func (r *Router) forwardPublish(ctx context.Context, targetNode string, req *pb.PublishRequest, hopCount int32) (*pb.PublishResponse, error) {
	if hopCount > maxHopCount {
		return nil, status.Errorf(codes.ResourceExhausted,
			"forwarding loop detected: hop_count %d exceeds max %d", hopCount, maxHopCount)
	}

	client, addr, err := r.getInternalClient(targetNode)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "cannot reach owner node %s: %v", targetNode, err)
	}

	fctx, cancel := context.WithTimeout(ctx, defaultForwardTimeout)
	defer cancel()

	resp, err := client.Forward(fctx, &pb.ForwardPublishRequest{
		Request:      req,
		HopCount:     hopCount + 1,
		SourceNodeId: r.nodeID,
	})
	if err != nil {
		r.logger.Warn("forward publish failed",
			"target_node", targetNode,
			"target_addr", addr,
			"topic", req.GetTopic(),
			"err", err,
		)

		// Retry once if the ring changed (owner may have changed)
		newOwner := r.ring.GetOwner(req.GetTopic())
		if newOwner != "" && newOwner != targetNode && newOwner != r.nodeID {
			r.logger.Info("retrying publish after ring change", "new_owner", newOwner, "topic", req.GetTopic())
			return r.forwardPublish(ctx, newOwner, req, hopCount)
		}
		if newOwner == r.nodeID {
			return r.localServer.publishLocal(ctx, req)
		}
		return nil, status.Errorf(codes.Unavailable, "owner node %s unavailable and no replacement: %v", targetNode, err)
	}
	return resp, nil
}

// ----------------------------------------------------------------------------
// Subscribe routing
// ----------------------------------------------------------------------------

// RouteSubscribe routes a Subscribe RPC to the topic's owner.
// If this node owns the topic, it subscribes locally.
// Otherwise it opens a ForwardSubscribe stream to the owner and proxies messages.
func (r *Router) RouteSubscribe(req *pb.SubscribeRequest, stream pb.Broker_SubscribeServer) error {
	owner := r.ring.GetOwner(req.GetTopic())
	if owner == "" {
		return status.Error(codes.Unavailable, "no node available for topic (ring is empty)")
	}

	if owner == r.nodeID {
		return r.localServer.subscribeLocal(req, stream)
	}

	return r.forwardSubscribe(stream.Context(), owner, req, stream, 0)
}

// forwardSubscribe opens a ForwardSubscribe stream to targetNode and proxies messages
// to the client's stream.
func (r *Router) forwardSubscribe(ctx context.Context, targetNode string, req *pb.SubscribeRequest, downstream pb.Broker_SubscribeServer, hopCount int32) error {
	if hopCount > maxHopCount {
		return status.Errorf(codes.ResourceExhausted,
			"forwarding loop detected on subscribe: hop_count %d exceeds max %d", hopCount, maxHopCount)
	}

	client, addr, err := r.getInternalClient(targetNode)
	if err != nil {
		return status.Errorf(codes.Unavailable, "cannot reach owner node %s: %v", targetNode, err)
	}

	r.logger.Info("forwarding subscribe",
		"topic", req.GetTopic(),
		"subscriber_id", req.GetSubscriberId(),
		"target_node", targetNode,
		"target_addr", addr,
	)

	upstream, err := client.ForwardSubscribe(ctx, req)
	if err != nil {
		return status.Errorf(codes.Unavailable, "ForwardSubscribe to %s failed: %v", targetNode, err)
	}

	// Proxy messages from the owner's stream to the client's stream.
	for {
		msg, err := upstream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := downstream.Send(msg); err != nil {
			return err
		}
	}
}

// ----------------------------------------------------------------------------
// Ack routing
// ----------------------------------------------------------------------------

// RouteAck routes an Ack RPC to the topic's owner.
// If this node owns the topic, it processes the ack locally.
// Otherwise it forwards the ack to the owner via the public Broker.Ack RPC
// (ack forwarding does not need the Internal service – we reuse Broker connections).
func (r *Router) RouteAck(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	owner := r.ring.GetOwner(req.GetTopic())
	if owner == "" || owner == r.nodeID {
		return r.localServer.ackLocal(ctx, req)
	}
	return r.forwardAck(ctx, owner, req)
}

func (r *Router) forwardAck(ctx context.Context, targetNode string, req *pb.AckRequest) (*pb.AckResponse, error) {
	brokerClient, _, err := r.getBrokerClient(targetNode)
	if err != nil {
		// Fall back to local handling rather than dropping the ack
		r.logger.Warn("cannot reach owner for ack forwarding; handling locally", "target_node", targetNode, "err", err)
		return r.localServer.ackLocal(ctx, req)
	}

	fctx, cancel := context.WithTimeout(ctx, defaultForwardTimeout)
	defer cancel()

	resp, err := brokerClient.Ack(fctx, req)
	if err != nil {
		r.logger.Warn("ack forward failed; handling locally", "target_node", targetNode, "err", err)
		return r.localServer.ackLocal(ctx, req)
	}
	return resp, nil
}

// ----------------------------------------------------------------------------
// InternalServer implementation – handles incoming forwarded requests
// ----------------------------------------------------------------------------

// Heartbeat implements pb.InternalServer: delegates heartbeat handling to membership.
func (r *Router) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if r.membership != nil {
		return r.membership.Heartbeat(ctx, req)
	}
	return &pb.HeartbeatResponse{
		NodeId: r.nodeID,
		Ok:     true,
	}, nil
}

// Forward implements pb.InternalServer: handles a ForwardPublish arriving from a peer.
func (r *Router) Forward(ctx context.Context, req *pb.ForwardPublishRequest) (*pb.PublishResponse, error) {
	if req.GetHopCount() > maxHopCount {
		return nil, status.Errorf(codes.ResourceExhausted,
			"hop_count %d exceeds max forwarding limit of %d", req.GetHopCount(), maxHopCount)
	}

	inner := req.GetRequest()
	if inner == nil || inner.GetTopic() == "" {
		return nil, status.Error(codes.InvalidArgument, "ForwardPublishRequest.request is required")
	}

	r.logger.Debug("handling forwarded Publish",
		"topic", inner.GetTopic(),
		"from_node", req.GetSourceNodeId(),
		"hop_count", req.GetHopCount(),
	)

	// Always handle locally: the forwarding node already resolved the owner.
	return r.localServer.publishLocal(ctx, inner)
}

// ForwardSubscribe implements pb.InternalServer: handles a forwarded Subscribe stream from a peer.
func (r *Router) ForwardSubscribe(req *pb.SubscribeRequest, stream pb.Internal_ForwardSubscribeServer) error {
	r.logger.Debug("handling forwarded Subscribe",
		"topic", req.GetTopic(),
		"subscriber_id", req.GetSubscriberId(),
	)
	// Wrap the internal stream as a Broker_SubscribeServer so we can call localServer.Subscribe.
	proxy := &internalSubscribeProxy{stream: stream}
	return r.localServer.subscribeLocal(req, proxy)
}

// internalSubscribeProxy adapts an Internal_ForwardSubscribeServer to the
// Broker_SubscribeServer interface so the local broker server can handle it transparently.
type internalSubscribeProxy struct {
	stream pb.Internal_ForwardSubscribeServer
}

func (p *internalSubscribeProxy) Send(msg *pb.Message) error     { return p.stream.Send(msg) }
func (p *internalSubscribeProxy) Context() context.Context       { return p.stream.Context() }
func (p *internalSubscribeProxy) SetHeader(md metadata.MD) error { return nil }
func (p *internalSubscribeProxy) SendHeader(md metadata.MD) error { return nil }
func (p *internalSubscribeProxy) SetTrailer(_ metadata.MD)        {}
func (p *internalSubscribeProxy) SendMsg(m interface{}) error    { return p.stream.SendMsg(m) }
func (p *internalSubscribeProxy) RecvMsg(m interface{}) error    { return p.stream.RecvMsg(m) }

// ----------------------------------------------------------------------------
// Connection pool helpers
// ----------------------------------------------------------------------------

// getInternalClient returns a reusable InternalClient for the given peer node ID.
func (r *Router) getInternalClient(nodeID string) (pb.InternalClient, string, error) {
	e, addr, err := r.getOrCreateConn(nodeID)
	if err != nil {
		return nil, addr, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client == nil {
		e.client = pb.NewInternalClient(e.conn)
	}
	return e.client, addr, nil
}

// getBrokerClient returns a reusable BrokerClient for the given peer node ID.
func (r *Router) getBrokerClient(nodeID string) (pb.BrokerClient, string, error) {
	e, addr, err := r.getOrCreateConn(nodeID)
	if err != nil {
		return nil, addr, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.broker == nil {
		e.broker = pb.NewBrokerClient(e.conn)
	}
	return e.broker, addr, nil
}

// getOrCreateConn lazily initialises the pool entry for nodeID.
func (r *Router) getOrCreateConn(nodeID string) (*connEntry, string, error) {
	// Fast path: already connected
	r.poolMu.RLock()
	e, ok := r.pool[nodeID]
	r.poolMu.RUnlock()
	if ok {
		return e, e.state(), nil
	}

	// Slow path: create a new connection
	r.poolMu.Lock()
	defer r.poolMu.Unlock()

	// Double-check under write lock
	if e, ok = r.pool[nodeID]; ok {
		return e, e.state(), nil
	}

	// Look up the peer address from membership
	addr := r.peerAddr(nodeID)
	if addr == "" {
		return nil, "", fmt.Errorf("unknown node %q: no address in peer list", nodeID)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if r.customDialer != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(r.customDialer))
	}

	target := addr
	if r.customDialer != nil {
		target = "passthrough:///" + addr
	}

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, addr, fmt.Errorf("dial %s (%s): %w", nodeID, addr, err)
	}

	e = &connEntry{conn: conn}
	r.pool[nodeID] = e
	return e, addr, nil
}

// peerAddr resolves the gRPC address of nodeID by consulting membership and config.
func (r *Router) peerAddr(nodeID string) string {
	if r.membership != nil {
		if s, ok := r.membership.GetPeerState(nodeID); ok && s.Address != "" {
			return s.Address
		}
	}
	// Fall back to config peers list
	if r.cfg != nil {
		for _, p := range r.cfg.Peers {
			if p.ID == nodeID {
				return p.Address
			}
		}
	}
	return ""
}

// state is a helper to get the address from a connEntry without locking the pool again.
func (e *connEntry) state() string {
	// addr is immutable after creation; we can read without lock
	if e.conn != nil {
		return e.conn.Target()
	}
	return ""
}
