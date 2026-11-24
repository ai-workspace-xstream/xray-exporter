package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xtls/xray-core/app/stats/command"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"xray-exporter/internal/xrayembed"
)

// StatsReader abstracts reading traffic and runtime statistics from Xray,
// supporting both direct in-memory access (embedded mode) and legacy gRPC polling.
type StatsReader interface {
	QueryStats(ctx context.Context, pattern string, reset bool) ([]*command.Stat, error)
	GetSysStats(ctx context.Context) (*command.SysStatsResponse, error)
	Close() error
}

type inprocStatsReader struct {
	engine     *xrayembed.Engine
	ownsEngine bool
}

func newInprocStatsReader(engine *xrayembed.Engine, ownsEngine bool) *inprocStatsReader {
	return &inprocStatsReader{
		engine:     engine,
		ownsEngine: ownsEngine,
	}
}

func (r *inprocStatsReader) QueryStats(ctx context.Context, pattern string, reset bool) ([]*command.Stat, error) {
	if r.engine == nil {
		return nil, xrayembed.ErrNotRunning
	}
	return r.engine.QueryStats(ctx, pattern, reset)
}

func (r *inprocStatsReader) GetSysStats(ctx context.Context) (*command.SysStatsResponse, error) {
	if r.engine == nil {
		return nil, xrayembed.ErrNotRunning
	}
	return r.engine.GetSysStats(ctx)
}

func (r *inprocStatsReader) Close() error {
	if r.ownsEngine && r.engine != nil {
		return r.engine.Close()
	}
	return nil
}

type grpcStatsReader struct {
	endpoint string
	conn     *grpc.ClientConn
	client   command.StatsServiceClient
}

func newGRPCStatsReader(endpoint string) (*grpcStatsReader, error) {
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}
	return &grpcStatsReader{
		endpoint: endpoint,
		conn:     conn,
		client:   command.NewStatsServiceClient(conn),
	}, nil
}

func (r *grpcStatsReader) QueryStats(ctx context.Context, pattern string, reset bool) ([]*command.Stat, error) {
	resp, err := callWithRetry(ctx, func() (any, error) {
		return r.client.QueryStats(ctx, &command.QueryStatsRequest{
			Pattern: pattern,
			Reset_:  reset,
		})
	})
	if err != nil {
		return nil, err
	}
	return resp.(*command.QueryStatsResponse).GetStat(), nil
}

func (r *grpcStatsReader) GetSysStats(ctx context.Context) (*command.SysStatsResponse, error) {
	resp, err := callWithRetry(ctx, func() (any, error) {
		return r.client.GetSysStats(ctx, &command.SysStatsRequest{})
	})
	if err != nil {
		return nil, err
	}
	return resp.(*command.SysStatsResponse), nil
}

func (r *grpcStatsReader) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// callWithRetry implements exponential backoff retry for gRPC calls to handle
// temporary network issues or external Xray restarts.
func callWithRetry(ctx context.Context, fn func() (any, error)) (any, error) {
	const maxRetries = 3
	const baseDelay = 100 * time.Millisecond

	for attempt := range maxRetries {
		resp, err := fn()
		if err == nil {
			return resp, nil
		}

		if attempt == maxRetries-1 {
			return nil, err
		}

		delay := baseDelay * time.Duration(1<<attempt)
		logrus.WithError(err).WithField("attempt", attempt+1).WithField("delay", delay).Debug("gRPC call failed, retrying")
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("max retries exceeded")
}
