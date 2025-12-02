package server

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	//nolint:gosec // only exposed if pprofAddr config is set
	_ "net/http/pprof"

	"github.com/ethpandaops/execution-processor/pkg/api"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/observability"
	"github.com/ethpandaops/execution-processor/pkg/processor"
	"github.com/ethpandaops/execution-processor/pkg/redis"
	"github.com/ethpandaops/execution-processor/pkg/state"
	r "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	log       logrus.FieldLogger
	config    *Config
	namespace string

	redis     *r.Client
	pool      *ethereum.Pool
	processor *processor.Manager
	state     *state.Manager

	pprofServer  *http.Server
	healthServer *http.Server
	apiServer    *http.Server
}

func NewServer(ctx context.Context, log logrus.FieldLogger, namespace string, config *Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	redisClient, err := redis.New(config.Redis)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis client: %w", err)
	}

	pool := ethereum.NewPool(log.WithField("component", "ethereum"), namespace, &config.Ethereum)

	stateManager, err := state.NewManager(ctx, log.WithField("component", "state"), &config.StateManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create state manager: %w", err)
	}

	p, err := processor.NewManager(log.WithField("component", "processor"), &config.Processors, pool, stateManager, redisClient, config.Redis.Prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to create processor manager: %w", err)
	}

	return &Server{
		config:    config,
		log:       log,
		namespace: namespace,
		redis:     redisClient,
		pool:      pool,
		state:     stateManager,
		processor: p,
	}, nil
}

func (s *Server) Start(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)

	// Log component states
	s.log.WithFields(logrus.Fields{
		"has_pool":      s.pool != nil,
		"has_state":     s.state != nil,
		"has_processor": s.processor != nil,
		"has_redis":     s.redis != nil,
	}).Debug("Server component states")

	// Start metrics server
	g.Go(func() error {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.log.WithField("panic", recovered).Error("Panic in metrics server goroutine")
			}
		}()

		observability.StartMetricsServer(ctx, s.config.MetricsAddr)
		<-ctx.Done()

		return nil
	})

	// Start pprof server if configured
	if s.config.PProfAddr != nil {
		g.Go(func() error {
			if err := s.startPProf(); err != nil && err != http.ErrServerClosed {
				return err
			}

			<-ctx.Done()

			return nil
		})
	}

	// Start health check server if configured
	if s.config.HealthCheckAddr != nil {
		g.Go(func() error {
			if err := s.startHealthCheck(); err != nil && err != http.ErrServerClosed {
				return err
			}

			<-ctx.Done()

			return nil
		})
	}

	// Start API server if configured
	if s.config.APIAddr != nil {
		g.Go(func() error {
			if err := s.startAPI(); err != nil && err != http.ErrServerClosed {
				return err
			}

			<-ctx.Done()

			return nil
		})
	}

	// Start ethereum pool
	g.Go(func() error {
		s.pool.Start(ctx)
		<-ctx.Done()

		return nil
	})

	g.Go(func() error {
		if err := s.state.Start(ctx); err != nil {
			s.log.WithError(err).Error("State manager start failed")

			return err
		}

		<-ctx.Done()

		return nil
	})

	// Start processor
	g.Go(func() error {
		return s.processor.Start(ctx)
	})

	// Wait for shutdown signal
	g.Go(func() error {
		<-ctx.Done()

		// Use a fresh context for cleanup since the current one is cancelled
		cleanupCtx := context.Background()

		return s.stop(cleanupCtx)
	})

	return g.Wait()
}

func (s *Server) stop(ctx context.Context) error {
	// Create a timeout context for cleanup
	cleanupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	s.log.Info("Starting graceful shutdown...")

	if s.processor != nil {
		s.log.Info("Stopping processor...")

		if err := s.processor.Stop(ctx); err != nil {
			s.log.WithError(err).Error("failed to stop processor")
		}
	}

	if s.state != nil {
		s.log.Info("Stopping state manager...")

		if err := s.state.Stop(ctx); err != nil {
			s.log.WithError(err).Error("failed to stop state manager")
		}
	}

	// Close Redis connection
	if s.redis != nil {
		s.log.Info("Closing Redis connection...")

		if err := s.redis.Close(); err != nil {
			s.log.WithError(err).Error("failed to close redis")
		}
	}

	// Shutdown HTTP servers
	if s.pprofServer != nil {
		if err := s.pprofServer.Shutdown(cleanupCtx); err != nil {
			s.log.WithError(err).Error("failed to shutdown pprof server")
		}
	}

	if s.healthServer != nil {
		if err := s.healthServer.Shutdown(cleanupCtx); err != nil {
			s.log.WithError(err).Error("failed to shutdown health server")
		}
	}

	if s.apiServer != nil {
		if err := s.apiServer.Shutdown(cleanupCtx); err != nil {
			s.log.WithError(err).Error("failed to shutdown API server")
		}
	}

	// Stop metrics server using observability package
	if err := observability.StopMetricsServer(cleanupCtx); err != nil {
		s.log.WithError(err).Error("failed to stop metrics server")
	}

	s.log.Info("Worker stopped gracefully")

	return nil
}

func (s *Server) startPProf() error {
	s.log.WithField("addr", *s.config.PProfAddr).Info("Starting pprof server")

	s.pprofServer = &http.Server{
		Addr:              *s.config.PProfAddr,
		ReadHeaderTimeout: 120 * time.Second,
	}

	return s.pprofServer.ListenAndServe()
}

func (s *Server) startHealthCheck() error {
	s.log.WithField("addr", *s.config.HealthCheckAddr).Info("Starting healthcheck server")

	s.healthServer = &http.Server{
		Addr:              *s.config.HealthCheckAddr,
		ReadHeaderTimeout: 120 * time.Second,
	}

	s.healthServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return s.healthServer.ListenAndServe()
}

func (s *Server) startAPI() error {
	s.log.WithField("addr", *s.config.APIAddr).Info("Starting API server")

	mux := http.NewServeMux()
	apiHandler := api.NewHandler(s.log.WithField("component", "api"), s.processor, s.pool)
	apiHandler.RegisterRoutes(mux)

	s.apiServer = &http.Server{
		Addr:              *s.config.APIAddr,
		Handler:           mux,
		ReadHeaderTimeout: 120 * time.Second,
	}

	return s.apiServer.ListenAndServe()
}
