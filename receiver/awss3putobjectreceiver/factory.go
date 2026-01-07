// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3putobjectreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3putobjectreceiver"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3putobjectreceiver/internal/metadata"
)

const (
	defaultEndpoint       = "localhost:9000"
	defaultParseFormat    = "lines"
	defaultAutoDecompress = true
	defaultMaxObjectSize  = 100 * 1024 * 1024 // 100MB
	defaultReadTimeout    = 30 * time.Second
	defaultWriteTimeout   = 30 * time.Second
)

// NewFactory creates the receiver factory
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ReadTimeout:    defaultReadTimeout,
		WriteTimeout:   defaultWriteTimeout,
		ParseFormat:    defaultParseFormat,
		AutoDecompress: defaultAutoDecompress,
		MaxObjectSize:  defaultMaxObjectSize,
		ResourceAttributes: map[string]string{
			"receiver.type": "s3",
		},
	}
}

func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	baseCfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	cfg := baseCfg.(*Config)
	return newLogsReceiver(params, *cfg, consumer)
}
