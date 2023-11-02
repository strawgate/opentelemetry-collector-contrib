// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package elasticsearchexporter contains an opentelemetry-collector exporter
// for Elasticsearch.
package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type elasticsearchMetricsExporter struct {
	logger *zap.Logger

	index        string
	dynamicIndex bool
	maxAttempts  int

	client      *esClientCurrent
	bulkIndexer esBulkIndexerCurrent
	model       mappingModel
}

func newMetricsExporter(logger *zap.Logger, cfg *Config) (*elasticsearchMetricsExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client, err := newElasticsearchClient(logger, cfg)
	if err != nil {
		return nil, err
	}

	bulkIndexer, err := newBulkIndexer(logger, client, cfg)
	if err != nil {
		return nil, err
	}

	maxAttempts := 1
	if cfg.Retry.Enabled {
		maxAttempts = cfg.Retry.MaxRequests
	}

	model := &encodeModel{dedup: cfg.Mapping.Dedup, dedot: cfg.Mapping.Dedot, mapping: cfg.Mapping.Mode}

	indexStr := cfg.MetricsIndex
	if cfg.Index != "" {
		indexStr = cfg.Index
	}
	esMetricsExp := &elasticsearchMetricsExporter{
		logger:      logger,
		client:      client,
		bulkIndexer: bulkIndexer,

		index:        indexStr,
		dynamicIndex: cfg.MetricsDynamicIndex.Enabled,
		maxAttempts:  maxAttempts,
		model:        model,
	}
	return esMetricsExp, nil
}

func (e *elasticsearchMetricsExporter) Shutdown(ctx context.Context) error {
	return e.bulkIndexer.Close(ctx)
}

func (e *elasticsearchMetricsExporter) pushMetricsData(ctx context.Context, ld pmetric.Metrics) error {
	var errs []error

	rls := ld.ResourceMetrics()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		resource := rl.Resource()
		ills := rl.ScopeMetrics()
		for j := 0; j < ills.Len(); j++ {
			scope := ills.At(j).Scope()
			metrics := ills.At(j).Metrics()

			if err := e.pushMetricRecord(ctx, resource, metrics, scope); err != nil {
				if cerr := ctx.Err(); cerr != nil {
					return cerr
				}

				errs = append(errs, err)
			}

		}
	}

	return errors.Join(errs...)
}

func (e *elasticsearchMetricsExporter) pushMetricRecord(ctx context.Context, resource pcommon.Resource, slice pmetric.MetricSlice, scope pcommon.InstrumentationScope) error {
	fIndex := e.index
	/* TODO: Implement dynamic index
	if e.dynamicIndex {
		prefix := getFromBothResourceAndAttribute(indexPrefix, resource, record)
		suffix := getFromBothResourceAndAttribute(indexSuffix, resource, record)

		fIndex = fmt.Sprintf("%s%s%s", prefix, fIndex, suffix)
	}
	*/

	documents, err := e.model.encodeMetric(resource, slice, scope)
	if err != nil {
		return fmt.Errorf("Failed to encode metric event: %w", err)
	}

	for _, document := range documents {
		err := pushDocuments(ctx, e.logger, fIndex, document.Bytes(), e.bulkIndexer, e.maxAttempts)
		if err != nil {
			return err
		}
	}

	return nil
}
