// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package s3receiver

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/s3receiver/internal/metadata"
)

func TestPutObjectEmitsLogs(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	cfg := &Config{
		ParseFormat:    "lines",
		AutoDecompress: true,
		MaxObjectSize:  10 * 1024 * 1024,
		ResourceAttributes: map[string]string{
			"service.name": "test",
		},
	}

	backend := newLogEmittingBackend(logger, sink, cfg, nil, "")
	faker := gofakes3.New(backend, gofakes3.WithAutoBucket(true))
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	// Upload test logs
	testData := "line1\nline2\nline3"
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/test-bucket/test-logs.txt", strings.NewReader(testData))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify logs were emitted
	require.Eventually(t, func() bool {
		return sink.LogRecordCount() == 3
	}, 5*time.Second, 100*time.Millisecond)

	logs := sink.AllLogs()[0]
	assert.Equal(t, 3, logs.LogRecordCount())

	// Check resource attributes
	resource := logs.ResourceLogs().At(0).Resource()
	val, ok := resource.Attributes().Get("s3.bucket")
	assert.True(t, ok)
	assert.Equal(t, "test-bucket", val.Str())

	keyVal, ok := resource.Attributes().Get("s3.key")
	assert.True(t, ok)
	assert.Equal(t, "test-logs.txt", keyVal.Str())
}

func TestJSONLogParsing(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	cfg := &Config{
		ParseFormat: "json",
		ResourceAttributes: map[string]string{
			"service.name": "test",
		},
	}

	backend := newLogEmittingBackend(logger, sink, cfg, nil, "")
	faker := gofakes3.New(backend, gofakes3.WithAutoBucket(true))
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	// Upload JSON lines
	jsonData := `{"level":"ERROR","message":"Something failed","user_id":"123"}
{"level":"INFO","message":"Request completed","duration_ms":42}`

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/logs/app.json", strings.NewReader(jsonData))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() == 2
	}, 5*time.Second, 100*time.Millisecond)

	// Verify severity was extracted
	record := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "ERROR", record.SeverityText())
	assert.Equal(t, plog.SeverityNumberError, record.SeverityNumber())
	assert.Equal(t, "Something failed", record.Body().Str())
}

func TestAutoParseFormat(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	cfg := &Config{
		ParseFormat: "auto",
		ResourceAttributes: map[string]string{
			"service.name": "test",
		},
	}

	backend := newLogEmittingBackend(logger, sink, cfg, nil, "")
	faker := gofakes3.New(backend, gofakes3.WithAutoBucket(true))
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	// Test with JSON content
	jsonData := `{"level":"WARN","message":"Warning message"}`
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/logs/json.log", strings.NewReader(jsonData))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 1
	}, 5*time.Second, 100*time.Millisecond)

	record := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "WARN", record.SeverityText())
}

func TestAutoParseFormatFallsBackToLines(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	cfg := &Config{
		ParseFormat: "auto",
		ResourceAttributes: map[string]string{
			"service.name": "test",
		},
	}

	backend := newLogEmittingBackend(logger, sink, cfg, nil, "")
	faker := gofakes3.New(backend, gofakes3.WithAutoBucket(true))
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	// Test with plain text content (not JSON)
	plainData := "This is a plain text log line\nAnother plain text line"
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/logs/plain.log", strings.NewReader(plainData))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 5*time.Second, 100*time.Millisecond)

	// Should have parsed as lines
	logs := sink.AllLogs()[0]
	assert.Equal(t, 2, logs.LogRecordCount())
}

func TestGzipDecompression(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	cfg := &Config{
		ParseFormat:    "lines",
		AutoDecompress: true,
		ResourceAttributes: map[string]string{
			"service.name": "test",
		},
	}

	backend := newLogEmittingBackend(logger, sink, cfg, nil, "")
	faker := gofakes3.New(backend, gofakes3.WithAutoBucket(true))
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	// Create gzip compressed data
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write([]byte("gzipped line 1\ngzipped line 2"))
	require.NoError(t, err)
	require.NoError(t, gw.Close())

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/logs/compressed.log.gz", &buf)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 2
	}, 5*time.Second, 100*time.Millisecond)

	logs := sink.AllLogs()[0]
	assert.Equal(t, 2, logs.LogRecordCount())

	// Verify content was decompressed
	record := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "gzipped line 1", record.Body().Str())
}

func TestBucketRestriction(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	cfg := &Config{
		BucketName:  "allowed-bucket",
		ParseFormat: "lines",
		ResourceAttributes: map[string]string{
			"service.name": "test",
		},
	}

	backend := newLogEmittingBackend(logger, sink, cfg, nil, "")
	faker := gofakes3.New(backend, gofakes3.WithAutoBucket(true))
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	// Try to upload to disallowed bucket
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/disallowed-bucket/test.log", strings.NewReader("test"))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	// Should fail with NoSuchBucket error
	assert.NotEqual(t, http.StatusOK, resp.StatusCode)

	// Upload to allowed bucket should succeed
	req2, err := http.NewRequest(http.MethodPut, ts.URL+"/allowed-bucket/test.log", strings.NewReader("test"))
	require.NoError(t, err)

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
}

func TestMaxObjectSize(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	cfg := &Config{
		ParseFormat:   "lines",
		MaxObjectSize: 100, // Only 100 bytes allowed
		ResourceAttributes: map[string]string{
			"service.name": "test",
		},
	}

	backend := newLogEmittingBackend(logger, sink, cfg, nil, "")
	faker := gofakes3.New(backend, gofakes3.WithAutoBucket(true))
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	// Try to upload an object larger than max size
	largeData := strings.Repeat("x", 200)
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/bucket/large.log", strings.NewReader(largeData))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	// Should fail with EntityTooLarge error
	assert.NotEqual(t, http.StatusOK, resp.StatusCode)
}

func TestReceiverLifecycle(t *testing.T) {
	sink := new(consumertest.LogsSink)
	settings := receivertest.NewNopSettings(metadata.Type)

	cfg := &Config{
		ServerConfig: confighttp.ServerConfig{
			Endpoint: "localhost:0", // Use port 0 to get a random available port
		},
		ParseFormat:    "lines",
		AutoDecompress: true,
		MaxObjectSize:  100 * 1024 * 1024,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		ResourceAttributes: map[string]string{
			"receiver.type": "s3",
		},
	}

	receiver, err := newLogsReceiver(settings, *cfg, sink)
	require.NoError(t, err)

	ctx := context.Background()
	err = receiver.Start(ctx, nil)
	require.NoError(t, err)

	err = receiver.Shutdown(ctx)
	require.NoError(t, err)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  func() Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: func() Config {
				cfg := Config{ParseFormat: "lines"}
				cfg.Endpoint = "localhost:9000"
				return cfg
			},
			wantErr: false,
		},
		{
			name: "missing endpoint",
			config: func() Config {
				return Config{ParseFormat: "lines"}
			},
			wantErr: true,
		},
		{
			name: "invalid parse format",
			config: func() Config {
				cfg := Config{ParseFormat: "invalid"}
				cfg.Endpoint = "localhost:9000"
				return cfg
			},
			wantErr: true,
		},
		{
			name: "negative max object size",
			config: func() Config {
				cfg := Config{MaxObjectSize: -1}
				cfg.Endpoint = "localhost:9000"
				return cfg
			},
			wantErr: true,
		},
		{
			name: "read timeout exceeds max",
			config: func() Config {
				cfg := Config{ReadTimeout: 61 * time.Second}
				cfg.Endpoint = "localhost:9000"
				return cfg
			},
			wantErr: true,
		},
		{
			name: "encoding and parse_format mutually exclusive",
			config: func() Config {
				cfg := Config{
					Encoding:    &Encoding{Extension: component.MustNewID("json_log")},
					ParseFormat: "json",
				}
				cfg.Endpoint = "localhost:9000"
				return cfg
			},
			wantErr: true,
		},
		{
			name: "valid config with encoding only",
			config: func() Config {
				cfg := Config{
					Encoding: &Encoding{Extension: component.MustNewID("json_log")},
				}
				cfg.Endpoint = "localhost:9000"
				return cfg
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.config()
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParserHelpers(t *testing.T) {
	t.Run("isGzip", func(t *testing.T) {
		// Valid gzip magic bytes
		assert.True(t, isGzip([]byte{0x1f, 0x8b, 0x08}))
		// Invalid data
		assert.False(t, isGzip([]byte{0x00, 0x00}))
		// Too short
		assert.False(t, isGzip([]byte{0x1f}))
	})

	t.Run("mapSeverity", func(t *testing.T) {
		assert.Equal(t, plog.SeverityNumberTrace, mapSeverity("TRACE"))
		assert.Equal(t, plog.SeverityNumberDebug, mapSeverity("debug"))
		assert.Equal(t, plog.SeverityNumberInfo, mapSeverity("INFO"))
		assert.Equal(t, plog.SeverityNumberInfo, mapSeverity("INFORMATION"))
		assert.Equal(t, plog.SeverityNumberWarn, mapSeverity("WARN"))
		assert.Equal(t, plog.SeverityNumberWarn, mapSeverity("WARNING"))
		assert.Equal(t, plog.SeverityNumberError, mapSeverity("ERROR"))
		assert.Equal(t, plog.SeverityNumberError, mapSeverity("ERR"))
		assert.Equal(t, plog.SeverityNumberFatal, mapSeverity("FATAL"))
		assert.Equal(t, plog.SeverityNumberFatal, mapSeverity("CRITICAL"))
		assert.Equal(t, plog.SeverityNumberUnspecified, mapSeverity("unknown"))
	})

	t.Run("parseTimestamp", func(t *testing.T) {
		// RFC3339
		ts := parseTimestamp("2024-01-15T10:30:00Z")
		assert.False(t, ts.IsZero())
		assert.Equal(t, 2024, ts.Year())

		// Unix timestamp (seconds)
		ts = parseTimestamp(float64(1705315800))
		assert.False(t, ts.IsZero())

		// Unix timestamp (milliseconds)
		ts = parseTimestamp(float64(1705315800000))
		assert.False(t, ts.IsZero())

		// Invalid
		ts = parseTimestamp("not a timestamp")
		assert.True(t, ts.IsZero())
	})
}

func TestMetadataHeaders(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	cfg := &Config{
		ParseFormat: "lines",
		ResourceAttributes: map[string]string{
			"service.name": "test",
		},
	}

	backend := newLogEmittingBackend(logger, sink, cfg, nil, "")
	faker := gofakes3.New(backend, gofakes3.WithAutoBucket(true))
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	// Upload with x-amz-meta-* headers
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/bucket/test.log", strings.NewReader("test line"))
	require.NoError(t, err)
	req.Header.Set("x-amz-meta-source", "test-source")
	req.Header.Set("x-amz-meta-environment", "testing")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 1
	}, 5*time.Second, 100*time.Millisecond)

	// Check that metadata was added as resource attributes
	resource := sink.AllLogs()[0].ResourceLogs().At(0).Resource()
	sourceVal, ok := resource.Attributes().Get("s3.meta.source")
	assert.True(t, ok)
	assert.Equal(t, "test-source", sourceVal.Str())
}

func TestEmptyLinesSkipped(t *testing.T) {
	sink := new(consumertest.LogsSink)
	logger := zaptest.NewLogger(t)

	cfg := &Config{
		ParseFormat: "lines",
		ResourceAttributes: map[string]string{
			"service.name": "test",
		},
	}

	backend := newLogEmittingBackend(logger, sink, cfg, nil, "")
	faker := gofakes3.New(backend, gofakes3.WithAutoBucket(true))
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	// Upload with empty lines
	testData := "line1\n\n\nline2\n   \nline3"
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/bucket/test.log", strings.NewReader(testData))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 3
	}, 5*time.Second, 100*time.Millisecond)

	// Should only have 3 log records (empty lines skipped)
	logs := sink.AllLogs()[0]
	assert.Equal(t, 3, logs.LogRecordCount())
}
