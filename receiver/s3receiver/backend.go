// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package s3receiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/s3receiver"

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/johannesboyne/gofakes3"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// logEmittingBackend implements gofakes3.Backend
type logEmittingBackend struct {
	logger   *zap.Logger
	consumer consumer.Logs
	config   *Config

	// Encoding extension unmarshaler (optional)
	unmarshaler    plog.Unmarshaler
	encodingSuffix string

	// Track created buckets (for ListBuckets)
	mu      sync.RWMutex
	buckets map[string]time.Time
}

func newLogEmittingBackend(logger *zap.Logger, consumer consumer.Logs, cfg *Config, unmarshaler plog.Unmarshaler, encodingSuffix string) *logEmittingBackend {
	return &logEmittingBackend{
		logger:         logger,
		consumer:       consumer,
		config:         cfg,
		unmarshaler:    unmarshaler,
		encodingSuffix: encodingSuffix,
		buckets:        make(map[string]time.Time),
	}
}

// PutObject receives S3 uploads and emits them as logs
func (b *logEmittingBackend) PutObject(
	bucketName, key string,
	meta map[string]string,
	input io.Reader,
	size int64,
	conditions *gofakes3.PutConditions,
) (gofakes3.PutObjectResult, error) {
	// Enforce bucket restriction if configured
	if b.config.BucketName != "" && bucketName != b.config.BucketName {
		return gofakes3.PutObjectResult{}, gofakes3.ErrorMessage(
			gofakes3.ErrNoSuchBucket,
			fmt.Sprintf("bucket %s not allowed", bucketName),
		)
	}

	// Read the entire body (respecting size limits)
	var reader io.Reader = input
	if b.config.MaxObjectSize > 0 {
		reader = io.LimitReader(input, b.config.MaxObjectSize+1)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		b.logger.Error("Failed to read object body", zap.Error(err))
		return gofakes3.PutObjectResult{}, gofakes3.ErrInternal
	}

	if b.config.MaxObjectSize > 0 && int64(len(data)) > b.config.MaxObjectSize {
		return gofakes3.PutObjectResult{}, gofakes3.ErrorMessage(
			gofakes3.ErrInvalidArgument,
			fmt.Sprintf("object exceeds max size of %d bytes", b.config.MaxObjectSize),
		)
	}

	// Auto-decompress gzip if enabled
	if b.config.AutoDecompress && isGzip(data) {
		decompressed, decompressErr := decompressGzip(data)
		if decompressErr != nil {
			b.logger.Warn("Gzip decompression failed, using raw data", zap.Error(decompressErr))
		} else {
			data = decompressed
		}
	}

	// Parse data into log records
	var logs plog.Logs
	var parseErr error

	// Use encoding extension if configured and suffix matches
	if b.unmarshaler != nil && (b.encodingSuffix == "" || strings.HasSuffix(key, b.encodingSuffix)) {
		logs, parseErr = b.unmarshaler.UnmarshalLogs(data)
		if parseErr != nil {
			b.logger.Error("Failed to unmarshal logs using encoding extension",
				zap.Error(parseErr),
				zap.String("key", key))
			return gofakes3.PutObjectResult{VersionID: ""}, nil
		}
		// Add resource attributes from config
		b.addResourceAttributes(logs, bucketName, key, meta)
	} else {
		// Fall back to built-in parsing
		logs = b.parseToLogs(bucketName, key, meta, data)
	}

	if logs.LogRecordCount() > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if consumeErr := b.consumer.ConsumeLogs(ctx, logs); consumeErr != nil {
			b.logger.Error("Failed to consume logs",
				zap.Error(consumeErr),
				zap.Int("log_count", logs.LogRecordCount()))
			// Return success to client anyway - we don't want to cause retries
			// that could lead to duplicate log processing
		} else {
			b.logger.Debug("Emitted logs from S3 upload",
				zap.String("bucket", bucketName),
				zap.String("key", key),
				zap.Int("log_count", logs.LogRecordCount()))
		}
	}

	return gofakes3.PutObjectResult{
		VersionID: "",
	}, nil
}

// parseToLogs converts raw bytes into plog.Logs
func (b *logEmittingBackend) parseToLogs(bucket, key string, meta map[string]string, data []byte) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()

	// Set resource attributes
	resource := resourceLogs.Resource()
	for k, v := range b.config.ResourceAttributes {
		resource.Attributes().PutStr(k, v)
	}
	resource.Attributes().PutStr("s3.bucket", bucket)
	resource.Attributes().PutStr("s3.key", key)

	// Add any x-amz-meta-* headers as resource attributes
	for k, v := range meta {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			attrKey := strings.TrimPrefix(strings.ToLower(k), "x-amz-meta-")
			resource.Attributes().PutStr("s3.meta."+attrKey, v)
		}
	}

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName("s3receiver")
	scopeLogs.Scope().SetVersion("1.0.0")

	now := pcommon.NewTimestampFromTime(time.Now())

	// Parse based on format
	switch b.config.ParseFormat {
	case "json":
		b.parseJSONLines(data, scopeLogs, now)
	case "auto":
		// Try JSON first, fall back to lines
		if !b.parseJSONLines(data, scopeLogs, now) {
			b.parseLines(data, scopeLogs, now)
		}
	default: // "lines"
		b.parseLines(data, scopeLogs, now)
	}

	return logs
}

// parseLines treats data as newline-separated log entries
func (b *logEmittingBackend) parseLines(data []byte, scopeLogs plog.ScopeLogs, observedTime pcommon.Timestamp) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	// Increase buffer for long lines
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	lineNum := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue // Skip empty lines
		}

		lineNum++
		logRecord := scopeLogs.LogRecords().AppendEmpty()
		logRecord.Body().SetStr(line)
		logRecord.SetObservedTimestamp(observedTime)
		logRecord.SetTimestamp(observedTime)
		logRecord.SetSeverityNumber(plog.SeverityNumberInfo)
		logRecord.SetSeverityText("INFO")
		logRecord.Attributes().PutInt("line.number", int64(lineNum))
	}
}

// parseJSONLines treats data as newline-delimited JSON
func (b *logEmittingBackend) parseJSONLines(data []byte, scopeLogs plog.ScopeLogs, observedTime pcommon.Timestamp) bool {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	parsedAny := false
	lineNum := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		lineNum++
		var jsonData map[string]any
		if err := json.Unmarshal(line, &jsonData); err != nil {
			// Not valid JSON - if in auto mode, return false to trigger fallback
			if !parsedAny {
				return false
			}
			// Otherwise log as raw string
			logRecord := scopeLogs.LogRecords().AppendEmpty()
			logRecord.Body().SetStr(string(line))
			logRecord.SetObservedTimestamp(observedTime)
			continue
		}

		parsedAny = true
		logRecord := scopeLogs.LogRecords().AppendEmpty()
		logRecord.SetObservedTimestamp(observedTime)

		// Extract common fields from JSON
		b.populateFromJSON(logRecord, jsonData, observedTime)
	}

	return parsedAny
}

// populateFromJSON extracts fields from JSON log entries
func (b *logEmittingBackend) populateFromJSON(record plog.LogRecord, data map[string]any, defaultTime pcommon.Timestamp) {
	// Try to extract timestamp from common field names
	for _, field := range []string{"timestamp", "time", "@timestamp", "ts"} {
		if ts, ok := data[field]; ok {
			if parsed := parseTimestamp(ts); !parsed.IsZero() {
				record.SetTimestamp(pcommon.NewTimestampFromTime(parsed))
				delete(data, field)
				break
			}
		}
	}
	if record.Timestamp() == 0 {
		record.SetTimestamp(defaultTime)
	}

	// Try to extract severity
	for _, field := range []string{"level", "severity", "log.level", "loglevel"} {
		if level, ok := data[field].(string); ok {
			record.SetSeverityText(level)
			record.SetSeverityNumber(mapSeverity(level))
			delete(data, field)
			break
		}
	}
	if record.SeverityNumber() == 0 {
		record.SetSeverityNumber(plog.SeverityNumberInfo)
		record.SetSeverityText("INFO")
	}

	// Try to extract message as body
	for _, field := range []string{"message", "msg", "log", "body"} {
		if msg, ok := data[field].(string); ok {
			record.Body().SetStr(msg)
			delete(data, field)
			break
		}
	}

	// If no message field found, set entire JSON as body
	if record.Body().Type() == pcommon.ValueTypeEmpty {
		body := record.Body().SetEmptyMap()
		insertMapToAttributes(data, body)
	}

	// Remaining fields become attributes
	for k, v := range data {
		insertValueToAttributes(record.Attributes(), k, v)
	}

	// Extract trace context if present
	if traceID, ok := data["trace_id"].(string); ok {
		if tid, err := parseTraceID(traceID); err == nil {
			record.SetTraceID(tid)
		}
	}
	if spanID, ok := data["span_id"].(string); ok {
		if sid, err := parseSpanID(spanID); err == nil {
			record.SetSpanID(sid)
		}
	}
}

// Helper methods for S3 backend interface (stubs)

func (b *logEmittingBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	bucketList := make([]gofakes3.BucketInfo, 0, len(b.buckets))
	for name, created := range b.buckets {
		bucketList = append(bucketList, gofakes3.BucketInfo{
			Name:         name,
			CreationDate: gofakes3.NewContentTime(created),
		})
	}
	return bucketList, nil
}

func (b *logEmittingBackend) CreateBucket(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.buckets[name]; exists {
		return gofakes3.ErrBucketAlreadyExists
	}
	b.buckets[name] = time.Now()
	return nil
}

func (b *logEmittingBackend) BucketExists(name string) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, exists := b.buckets[name]
	return exists, nil
}

func (b *logEmittingBackend) DeleteBucket(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.buckets, name)
	return nil
}

// HeadObject - not supported for log receiver, return no such key
func (b *logEmittingBackend) HeadObject(bucketName, key string) (*gofakes3.Object, error) {
	return nil, gofakes3.ErrNoSuchKey
}

// GetObject - not supported for log receiver
func (b *logEmittingBackend) GetObject(bucketName, key string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	return nil, gofakes3.ErrNoSuchKey
}

// DeleteObject - accepts but does nothing
func (b *logEmittingBackend) DeleteObject(bucketName, key string) (gofakes3.ObjectDeleteResult, error) {
	return gofakes3.ObjectDeleteResult{}, nil
}

// DeleteMulti - accepts but does nothing
func (b *logEmittingBackend) DeleteMulti(bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	return gofakes3.MultiDeleteResult{}, nil
}

// ListBucket - returns empty list
func (b *logEmittingBackend) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	return &gofakes3.ObjectList{
		CommonPrefixes: []gofakes3.CommonPrefix{},
		Contents:       []*gofakes3.Content{},
		IsTruncated:    false,
	}, nil
}

// ForceDeleteBucket deletes a bucket and all its contents
func (b *logEmittingBackend) ForceDeleteBucket(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.buckets, name)
	return nil
}

// CopyObject - not supported
func (b *logEmittingBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	return gofakes3.CopyObjectResult{}, gofakes3.ErrNotImplemented
}

// addResourceAttributes adds S3 metadata and configured resource attributes to logs
// parsed by an encoding extension
func (b *logEmittingBackend) addResourceAttributes(logs plog.Logs, bucket, key string, meta map[string]string) {
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		resource := logs.ResourceLogs().At(i).Resource()

		// Add configured resource attributes
		for k, v := range b.config.ResourceAttributes {
			resource.Attributes().PutStr(k, v)
		}

		// Add S3 object information
		resource.Attributes().PutStr("s3.bucket", bucket)
		resource.Attributes().PutStr("s3.key", key)

		// Add any x-amz-meta-* headers as resource attributes
		for k, v := range meta {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
				attrKey := strings.TrimPrefix(strings.ToLower(k), "x-amz-meta-")
				resource.Attributes().PutStr("s3.meta."+attrKey, v)
			}
		}
	}
}
