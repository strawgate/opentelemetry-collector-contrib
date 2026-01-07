// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package awss3putobjectreceiver

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3putobjectreceiver/internal/metadata"
)

// getReceiverAddr extracts the listening address from the receiver
func getReceiverAddr(r interface{}) string {
	if s3r, ok := r.(*s3LogReceiver); ok {
		return s3r.addr
	}
	return ""
}

// TestE2E_S3ReceiverFullFlow tests the complete flow of the S3 receiver
// Run with: go test -tags=e2e -v -run TestE2E
func TestE2E_S3ReceiverFullFlow(t *testing.T) {
	sink := new(consumertest.LogsSink)
	settings := receivertest.NewNopSettings(metadata.Type)

	// Create receiver with a random port
	cfg := &Config{
		ParseFormat:    "json",
		AutoDecompress: true,
		MaxObjectSize:  10 * 1024 * 1024,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		ResourceAttributes: map[string]string{
			"receiver.type":    "s3",
			"test.environment": "e2e",
		},
	}
	cfg.Endpoint = "localhost:0" // Use random available port

	receiver, err := newLogsReceiver(settings, *cfg, sink)
	require.NoError(t, err)

	ctx := context.Background()
	err = receiver.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, receiver.Shutdown(ctx))
	}()

	// Get the actual port from the concrete type
	s3receiver := receiver.(*s3LogReceiver)
	baseURL := fmt.Sprintf("http://%s", s3receiver.addr)
	t.Logf("S3 receiver listening on %s", baseURL)

	// Wait for server to be ready
	require.Eventually(t, func() bool {
		resp, err := http.Get(baseURL)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	}, 5*time.Second, 100*time.Millisecond, "Server failed to start")

	t.Run("JSON log parsing", func(t *testing.T) {
		sink.Reset()

		jsonData := `{"level":"ERROR","message":"Database connection failed","error_code":"DB001"}
{"level":"INFO","message":"Request processed successfully","duration_ms":42}
{"level":"WARN","message":"Cache miss","cache_key":"user:123"}`

		req, err := http.NewRequest(http.MethodPut, baseURL+"/logs-bucket/app/server.json", strings.NewReader(jsonData))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-amz-meta-application", "test-server")
		req.Header.Set("x-amz-meta-environment", "e2e-test")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		require.Eventually(t, func() bool {
			return sink.LogRecordCount() == 3
		}, 5*time.Second, 100*time.Millisecond)

		logs := sink.AllLogs()[0]
		assert.Equal(t, 3, logs.LogRecordCount())

		// Verify resource attributes
		resource := logs.ResourceLogs().At(0).Resource()
		bucket, _ := resource.Attributes().Get("s3.bucket")
		assert.Equal(t, "logs-bucket", bucket.Str())

		key, _ := resource.Attributes().Get("s3.key")
		assert.Equal(t, "app/server.json", key.Str())

		metaApp, _ := resource.Attributes().Get("s3.meta.application")
		assert.Equal(t, "test-server", metaApp.Str())

		receiverType, _ := resource.Attributes().Get("receiver.type")
		assert.Equal(t, "s3", receiverType.Str())

		// Verify first log record has correct severity
		record := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
		assert.Equal(t, "ERROR", record.SeverityText())
		assert.Equal(t, "Database connection failed", record.Body().Str())

		t.Logf("Successfully received %d log records", logs.LogRecordCount())
	})

	t.Run("Gzip compressed logs", func(t *testing.T) {
		sink.Reset()

		// Create gzipped JSON data
		jsonData := `{"level":"INFO","message":"Compressed log 1"}
{"level":"DEBUG","message":"Compressed log 2"}`

		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte(jsonData))
		require.NoError(t, err)
		require.NoError(t, gw.Close())

		req, err := http.NewRequest(http.MethodPut, baseURL+"/logs-bucket/compressed/app.json.gz", &buf)
		require.NoError(t, err)
		req.Header.Set("Content-Encoding", "gzip")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		require.Eventually(t, func() bool {
			return sink.LogRecordCount() == 2
		}, 5*time.Second, 100*time.Millisecond)

		t.Logf("Successfully received %d compressed log records", sink.LogRecordCount())
	})

	t.Run("Timestamp parsing", func(t *testing.T) {
		sink.Reset()

		jsonData := `{"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"Log with RFC3339 timestamp"}
{"@timestamp":"2024-01-15T10:31:00Z","level":"WARN","message":"Log with @timestamp"}`

		req, err := http.NewRequest(http.MethodPut, baseURL+"/logs-bucket/timed/events.json", strings.NewReader(jsonData))
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		require.Eventually(t, func() bool {
			return sink.LogRecordCount() == 2
		}, 5*time.Second, 100*time.Millisecond)

		t.Logf("Successfully received %d timestamped log records", sink.LogRecordCount())
	})

	t.Run("Bucket restriction", func(t *testing.T) {
		// Create a new receiver with bucket restriction
		restrictedSink := new(consumertest.LogsSink)
		restrictedCfg := &Config{
			BucketName:     "allowed-bucket",
			ParseFormat:    "json",
			AutoDecompress: true,
		}
		restrictedCfg.Endpoint = "localhost:0"

		restrictedReceiver, err := newLogsReceiver(settings, *restrictedCfg, restrictedSink)
		require.NoError(t, err)

		err = restrictedReceiver.Start(ctx, componenttest.NewNopHost())
		require.NoError(t, err)
		defer restrictedReceiver.Shutdown(ctx)

		restrictedURL := fmt.Sprintf("http://%s", restrictedReceiver.(*s3LogReceiver).addr)

		// Wait for server
		require.Eventually(t, func() bool {
			resp, err := http.Get(restrictedURL)
			if err != nil {
				return false
			}
			resp.Body.Close()
			return true
		}, 5*time.Second, 100*time.Millisecond)

		// Try disallowed bucket - should fail
		req, err := http.NewRequest(http.MethodPut, restrictedURL+"/wrong-bucket/test.json",
			strings.NewReader(`{"message":"test"}`))
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.NotEqual(t, http.StatusOK, resp.StatusCode)
		t.Logf("Disallowed bucket correctly rejected with status %d", resp.StatusCode)

		// Try allowed bucket - should succeed
		req2, err := http.NewRequest(http.MethodPut, restrictedURL+"/allowed-bucket/test.json",
			strings.NewReader(`{"message":"test"}`))
		require.NoError(t, err)

		resp2, err := http.DefaultClient.Do(req2)
		require.NoError(t, err)
		defer resp2.Body.Close()
		assert.Equal(t, http.StatusOK, resp2.StatusCode)
		t.Logf("Allowed bucket correctly accepted")
	})

	t.Run("Max object size", func(t *testing.T) {
		// Create a new receiver with size limit
		sizeSink := new(consumertest.LogsSink)
		sizeCfg := &Config{
			ParseFormat:   "json",
			MaxObjectSize: 100, // Only 100 bytes
		}
		sizeCfg.Endpoint = "localhost:0"

		sizeReceiver, err := newLogsReceiver(settings, *sizeCfg, sizeSink)
		require.NoError(t, err)

		err = sizeReceiver.Start(ctx, componenttest.NewNopHost())
		require.NoError(t, err)
		defer sizeReceiver.Shutdown(ctx)

		sizeURL := fmt.Sprintf("http://%s", sizeReceiver.(*s3LogReceiver).addr)

		// Wait for server
		require.Eventually(t, func() bool {
			resp, err := http.Get(sizeURL)
			if err != nil {
				return false
			}
			resp.Body.Close()
			return true
		}, 5*time.Second, 100*time.Millisecond)

		// Try large object - should fail
		largeData := strings.Repeat(`{"m":"x"}`, 20) // > 100 bytes
		req, err := http.NewRequest(http.MethodPut, sizeURL+"/bucket/large.json",
			strings.NewReader(largeData))
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.NotEqual(t, http.StatusOK, resp.StatusCode)
		t.Logf("Large object correctly rejected with status %d", resp.StatusCode)
	})

	t.Run("S3 list buckets", func(t *testing.T) {
		// Test that we can list buckets (basic S3 compatibility)
		req, err := http.NewRequest(http.MethodGet, baseURL+"/", nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, string(body), "ListAllMyBucketsResult")
		t.Logf("S3 ListBuckets API works correctly")
	})
}

// TestE2E_LinesFormat tests line-based log parsing
func TestE2E_LinesFormat(t *testing.T) {
	sink := new(consumertest.LogsSink)
	settings := receivertest.NewNopSettings(metadata.Type)

	cfg := &Config{
		ParseFormat:    "lines",
		AutoDecompress: true,
	}
	cfg.Endpoint = "localhost:0"

	recv, err := newLogsReceiver(settings, *cfg, sink)
	require.NoError(t, err)

	ctx := context.Background()
	err = recv.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
	defer recv.Shutdown(ctx)

	baseURL := fmt.Sprintf("http://%s", recv.(*s3LogReceiver).addr)

	require.Eventually(t, func() bool {
		resp, err := http.Get(baseURL)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	}, 5*time.Second, 100*time.Millisecond)

	plainText := `2024-01-15 10:30:00 INFO Application started
2024-01-15 10:30:01 DEBUG Loading configuration
2024-01-15 10:30:02 ERROR Failed to connect to database
2024-01-15 10:30:03 WARN Retrying connection`

	req, err := http.NewRequest(http.MethodPut, baseURL+"/logs/app.log", strings.NewReader(plainText))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() == 4
	}, 5*time.Second, 100*time.Millisecond)

	t.Logf("Successfully parsed %d lines from plain text log", sink.LogRecordCount())
}

// TestE2E_AutoFormat tests automatic format detection
func TestE2E_AutoFormat(t *testing.T) {
	sink := new(consumertest.LogsSink)
	settings := receivertest.NewNopSettings(metadata.Type)

	cfg := &Config{
		ParseFormat:    "auto",
		AutoDecompress: true,
	}
	cfg.Endpoint = "localhost:0"

	recv, err := newLogsReceiver(settings, *cfg, sink)
	require.NoError(t, err)

	ctx := context.Background()
	err = recv.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
	defer recv.Shutdown(ctx)

	baseURL := fmt.Sprintf("http://%s", recv.(*s3LogReceiver).addr)

	require.Eventually(t, func() bool {
		resp, err := http.Get(baseURL)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	}, 5*time.Second, 100*time.Millisecond)

	// Test with JSON - should auto-detect
	jsonData := `{"level":"INFO","message":"Auto-detected JSON"}`
	req, err := http.NewRequest(http.MethodPut, baseURL+"/logs/auto.log", strings.NewReader(jsonData))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 1
	}, 5*time.Second, 100*time.Millisecond)

	// Verify it was parsed as JSON (has severity)
	record := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, "INFO", record.SeverityText())

	t.Logf("Auto-format correctly detected JSON")

	// Test with plain text - should fall back to lines
	sink.Reset()
	plainText := "This is plain text, not JSON"
	req2, err := http.NewRequest(http.MethodPut, baseURL+"/logs/plain.log", strings.NewReader(plainText))
	require.NoError(t, err)

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 1
	}, 5*time.Second, 100*time.Millisecond)

	t.Logf("Auto-format correctly fell back to lines for plain text")
}
