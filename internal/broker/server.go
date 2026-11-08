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

// Publish receives a message payload and publishes it via Manager.
func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
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
	topic := req.GetTopic()
	if topic == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
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
	if err := s.manager.AddSubscriber(topic, sub); err != nil {
		s.logger.Error("failed to add subscriber", "topic", topic, "subscriber_id", subID, "err", err)
		return status.Errorf(codes.Internal, "add subscriber error: %v", err)
	}

	s.logger.Info("subscriber connected",
		"topic", topic,
		"subscriber_id", subID,
		"group", req.GetGroup(),
		"from_seq", req.GetFromSeq(),
	)

	defer func() {
		_ = s.manager.RemoveSubscriber(topic, subID)
		s.logger.Info("subscriber disconnected and cleaned up",
			"topic", topic,
			"subscriber_id", subID,
		)
	}()

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
	s.logger.Info("received ack",
		"topic", req.GetTopic(),
		"subscriber_id", req.GetSubscriberId(),
		"seq", req.GetSeq(),
		"group", req.GetGroup(),
	)
	s.manager.Acknowledge(req.GetTopic(), req.GetSubscriberId(), req.GetSeq())
	return &pb.AckResponse{}, nil
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

