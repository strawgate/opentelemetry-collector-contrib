// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3putobjectreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3putobjectreceiver"

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/johannesboyne/gofakes3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3putobjectreceiver/internal/metadata"
)

var errNilLogsConsumer = errors.New("missing a logs consumer")

type s3LogReceiver struct {
	settings   receiver.Settings
	cfg        *Config
	consumer   consumer.Logs
	server     *http.Server
	backend    *logEmittingBackend
	shutdownWG sync.WaitGroup
	obsrecv    *receiverhelper.ObsReport
	addr       string // actual listening address after Start()
}

func newLogsReceiver(params receiver.Settings, cfg Config, consumer consumer.Logs) (receiver.Logs, error) {
	if consumer == nil {
		return nil, errNilLogsConsumer
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	transport := "http"
	if cfg.TLS.HasValue() {
		transport = "https"
	}

	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             params.ID,
		Transport:              transport,
		ReceiverCreateSettings: params,
	})
	if err != nil {
		return nil, err
	}

	return &s3LogReceiver{
		settings: params,
		cfg:      &cfg,
		consumer: consumer,
		obsrecv:  obsrecv,
	}, nil
}

// Start function manages receiver startup tasks. Part of the receiver.Logs interface.
func (r *s3LogReceiver) Start(ctx context.Context, host component.Host) error {
	// noop if already started
	if r.server != nil && r.server.Handler != nil {
		return nil
	}

	r.settings.Logger.Info("Starting S3 log receiver", zap.String("endpoint", r.cfg.Endpoint))

	// Look up encoding extension if configured
	var unmarshaler plog.Unmarshaler
	var encodingSuffix string
	if r.cfg.Encoding != nil {
		extensions := host.GetExtensions()
		ext, ok := extensions[r.cfg.Encoding.Extension]
		if !ok {
			return fmt.Errorf("encoding extension %q not found", r.cfg.Encoding.Extension)
		}
		unmarshaler, ok = ext.(plog.Unmarshaler)
		if !ok {
			return fmt.Errorf("encoding extension %q does not implement plog.Unmarshaler", r.cfg.Encoding.Extension)
		}
		encodingSuffix = r.cfg.Encoding.Suffix
		r.settings.Logger.Info("Using encoding extension for log parsing",
			zap.String("extension", r.cfg.Encoding.Extension.String()),
			zap.String("suffix", encodingSuffix))
	}

	// Create custom backend that emits logs
	r.backend = newLogEmittingBackend(r.settings.Logger, r.consumer, r.cfg, unmarshaler, encodingSuffix)

	// Create gofakes3 server with auto-bucket creation enabled
	faker := gofakes3.New(r.backend,
		gofakes3.WithAutoBucket(true), // Auto-create buckets on first use
	)

	// Create listener
	listener, err := net.Listen("tcp", r.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", r.cfg.Endpoint, err)
	}

	// Store the actual listening address (useful when using port 0)
	r.addr = listener.Addr().String()

	// Create HTTP server
	r.server = &http.Server{
		Addr:              r.addr,
		Handler:           faker.Server(),
		ReadTimeout:       r.cfg.ReadTimeout,
		ReadHeaderTimeout: r.cfg.ReadTimeout,
		WriteTimeout:      r.cfg.WriteTimeout,
		IdleTimeout:       120 * time.Second,
	}

	// Start serving
	r.shutdownWG.Add(1)
	go func() {
		defer r.shutdownWG.Done()
		r.settings.Logger.Info("S3 endpoint listening", zap.String("address", listener.Addr().String()))
		if errHTTP := r.server.Serve(listener); !errors.Is(errHTTP, http.ErrServerClosed) && errHTTP != nil {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errHTTP))
		}
	}()

	return nil
}

// Shutdown function manages receiver shutdown tasks. Part of the receiver.Logs interface.
func (r *s3LogReceiver) Shutdown(ctx context.Context) error {
	r.settings.Logger.Info("Shutting down S3 log receiver")
	if r.server == nil {
		return nil
	}

	err := r.server.Shutdown(ctx)
	r.shutdownWG.Wait()
	return err
}

// obsReportLogsEmitted reports the number of logs emitted for observability
func (r *s3LogReceiver) obsReportLogsEmitted(ctx context.Context, logCount int, err error) {
	r.obsrecv.EndLogsOp(ctx, metadata.Type.String(), logCount, err)
}
