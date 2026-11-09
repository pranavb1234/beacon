package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/pb"
)

// Server implements the gRPC Broker service.
type Server struct {
	pb.UnimplementedBrokerServer

	manager    *Manager
	cfg        *config.Config
	logger     *slog.Logger
	grpcServer *grpc.Server

	// router, when set, intercepts Publish/Subscribe/Ack and routes them to
	// the topic's owner node. Set via AttachRouter after construction.
	router *Router

	mu       sync.Mutex
	listener net.Listener
}

// NewServer creates a new gRPC Broker Server.
func NewServer(manager *Manager, cfg *config.Config, logger *slog.Logger) *Server {
	if cfg == nil {
		cfg = config.Default()
	}
	if logger == nil {
		logger = slog.Default()
	}

	grpcServer := grpc.NewServer()
	s := &Server{
		manager:    manager,
		cfg:        cfg,
		logger:     logger,
		grpcServer: grpcServer,
	}

	pb.RegisterBrokerServer(grpcServer, s)
	return s
}

// AttachRouter wires a Router into the server so that Publish, Subscribe, and Ack
// RPCs are routed to the topic's consistent-hash owner rather than always handled locally.
// Also registers the router as the InternalServer (Forward / ForwardSubscribe handler).
func (s *Server) AttachRouter(r *Router) {
	s.mu.Lock()
	s.router = r
	s.mu.Unlock()
	pb.RegisterInternalServer(s.grpcServer, r)
}

// RegisterInternalServer registers an implementation of the cluster Internal gRPC service.
func (s *Server) RegisterInternalServer(srv pb.InternalServer) {
	pb.RegisterInternalServer(s.grpcServer, srv)
}

// Publish receives a message payload and publishes it via Manager.
func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	s.mu.Lock()
	r := s.router
	s.mu.Unlock()

	if r != nil {
		return r.RoutePublish(ctx, req)
	}

	return s.publishLocal(ctx, req)
}

// publishLocal publishes directly on this node's manager without routing.
func (s *Server) publishLocal(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	if req.GetTopic() == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	seq, err := s.manager.Publish(req.GetTopic(), req.GetPayload())
	if err != nil {
		s.logger.Error("failed to publish message", "topic", req.GetTopic(), "err", err)
		return nil, status.Errorf(codes.Internal, "publish error: %v", err)
	}

	s.logger.Debug("published message",
		"topic", req.GetTopic(),
		"seq", seq,
		"payload_size", len(req.GetPayload()),
	)

	return &pb.PublishResponse{Sequence: seq}, nil
}

// Subscribe opens a server-streaming connection to stream topic messages to the client.
func (s *Server) Subscribe(req *pb.SubscribeRequest, stream pb.Broker_SubscribeServer) error {
	s.mu.Lock()
	r := s.router
	s.mu.Unlock()

	if r != nil {
		return r.RouteSubscribe(req, stream)
	}

	return s.subscribeLocal(req, stream)
}

// subscribeLocal handles a Subscribe RPC directly on this node's manager (no routing).
func (s *Server) subscribeLocal(req *pb.SubscribeRequest, stream pb.Broker_SubscribeServer) error {
	topic := req.GetTopic()
	if topic == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}

	fromSeq := req.GetFromSeq()
	group := req.GetGroup()

	t := s.manager.GetOrCreateTopic(topic)

	// If consumer group member with from_seq = 0, start from group committed offset + 1
	if group != "" && fromSeq == 0 {
		fromSeq = s.manager.GetGroupCommittedOffset(topic, group) + 1
	}

	// If from_seq is older than oldest available in the buffer, return OUT_OF_RANGE error
	if fromSeq > 0 && t.RingBuffer().Len() > 0 {
		oldest := t.RingBuffer().OldestSeq()
		if fromSeq < oldest {
			return status.Errorf(codes.OutOfRange,
				"requested sequence %d is older than oldest available sequence %d",
				fromSeq, oldest)
		}
	}

	subID := req.GetSubscriberId()
	if subID == "" {
		subID = fmt.Sprintf("sub-%d", time.Now().UnixNano())
	}

	bufSize := s.cfg.SubscriberBufferSize
	if bufSize <= 0 {
		bufSize = 1000
	}

	sub := NewSubscriber(subID, bufSize)
	sub.GroupID = group

	var replayMsgs []*pb.Message
	var boundarySeq uint64

	// Boundary synchronization:
	// Acquire topic lock to snapshot boundarySeq and register the subscriber atomically.
	// This prevents race conditions between ring buffer replay and incoming live publishes.
	t.mu.Lock()
	boundarySeq = t.RingBuffer().NewestSeq()
	if fromSeq > 0 && fromSeq <= boundarySeq {
		rawMsgs, ok := t.RingBuffer().GetFrom(fromSeq)
		if !ok {
			t.mu.Unlock()
			return status.Errorf(codes.OutOfRange,
				"requested sequence %d is older than oldest available sequence %d",
				fromSeq, t.RingBuffer().OldestSeq())
		}
		for _, m := range rawMsgs {
			if m.Seq <= boundarySeq {
				replayMsgs = append(replayMsgs, &pb.Message{
					Topic:       m.Topic,
					Seq:         m.Seq,
					Payload:     m.Payload,
					TimestampNs: m.TimestampNS,
				})
			}
		}
	}
	t.AddSubscriberLocked(sub)
	t.mu.Unlock()

	s.manager.startRetryScanner(sub)

	s.logger.Info("subscriber connected",
		"topic", topic,
		"subscriber_id", subID,
		"group", group,
		"from_seq", fromSeq,
		"replay_count", len(replayMsgs),
	)

	defer func() {
		_ = s.manager.RemoveSubscriber(topic, subID)
		s.logger.Info("subscriber disconnected and cleaned up",
			"topic", topic,
			"subscriber_id", subID,
		)
	}()

	// 1. Stream replayed messages first
	for _, m := range replayMsgs {
		if err := stream.Send(m); err != nil {
			return err
		}
	}

	// 2. Stream live messages, skipping any messages already replayed
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()

		case <-sub.Done:
			if err := sub.Err(); err != nil {
				return status.Errorf(codes.ResourceExhausted, "subscriber terminated: %v", err)
			}
			return nil

		case msg, ok := <-sub.Send:
			if !ok {
				if err := sub.Err(); err != nil {
					return status.Errorf(codes.ResourceExhausted, "subscriber channel closed: %v", err)
				}
				return nil
			}

			// Boundary check: skip live message if it was already sent during replay
			if msg.Seq <= boundarySeq {
				continue
			}

			pbMsg := &pb.Message{
				Topic:       msg.Topic,
				Seq:         msg.Seq,
				Payload:     msg.Payload,
				TimestampNs: msg.TimestampNS,
			}

			if err := stream.Send(pbMsg); err != nil {
				s.logger.Warn("failed to send message to stream",
					"subscriber_id", subID,
					"seq", msg.Seq,
					"err", err,
				)
				return err
			}
		}
	}
}

// Ack accepts, logs, and processes message acknowledgments.
func (s *Server) Ack(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	s.mu.Lock()
	r := s.router
	s.mu.Unlock()

	if r != nil {
		return r.RouteAck(ctx, req)
	}

	return s.ackLocal(ctx, req)
}

// ackLocal processes an Ack directly on this node's manager without routing.
func (s *Server) ackLocal(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	s.logger.Info("received ack",
		"topic", req.GetTopic(),
		"subscriber_id", req.GetSubscriberId(),
		"seq", req.GetSeq(),
		"group", req.GetGroup(),
	)
	s.manager.Acknowledge(req.GetTopic(), req.GetSubscriberId(), req.GetSeq())
	return &pb.AckResponse{}, nil
}

// GRPCServer returns the underlying gRPC server (for registering additional services).
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// Serve accepts incoming connections on the listener.
func (s *Server) Serve(lis net.Listener) error {
	s.mu.Lock()
	s.listener = lis
	s.mu.Unlock()

	s.logger.Info("broker gRPC server listening", "addr", lis.Addr().String())
	return s.grpcServer.Serve(lis)
}

// GracefulStop gracefully stops the gRPC server.
func (s *Server) GracefulStop() {
	s.logger.Info("initiating graceful stop of gRPC server")
	s.grpcServer.GracefulStop()
}

// Stop forcibly stops the gRPC server.
func (s *Server) Stop() {
	s.logger.Info("forcibly stopping gRPC server")
	s.grpcServer.Stop()
}

// Addr returns the network address the server is listening on.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return ""
}

