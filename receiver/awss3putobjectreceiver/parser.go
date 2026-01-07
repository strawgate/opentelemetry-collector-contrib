// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3putobjectreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3putobjectreceiver"

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// isGzip checks if data starts with gzip magic bytes
func isGzip(data []byte) bool {
	return len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

// decompressGzip decompresses gzip data
func decompressGzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// mapSeverity converts string severity to plog.SeverityNumber
func mapSeverity(level string) plog.SeverityNumber {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "TRACE":
		return plog.SeverityNumberTrace
	case "DEBUG":
		return plog.SeverityNumberDebug
	case "INFO", "INFORMATION":
		return plog.SeverityNumberInfo
	case "WARN", "WARNING":
		return plog.SeverityNumberWarn
	case "ERROR", "ERR":
		return plog.SeverityNumberError
	case "FATAL", "CRITICAL", "EMERGENCY", "PANIC":
		return plog.SeverityNumberFatal
	default:
		return plog.SeverityNumberUnspecified
	}
}

// parseTimestamp attempts to parse various timestamp formats
func parseTimestamp(value any) time.Time {
	switch v := value.(type) {
	case string:
		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05.000Z",
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05.000",
			"2006-01-02 15:04:05",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}
	case float64:
		// Unix timestamp (seconds or milliseconds)
		if v > 1e12 {
			return time.UnixMilli(int64(v))
		}
		return time.Unix(int64(v), 0)
	case int64:
		if v > 1e12 {
			return time.UnixMilli(v)
		}
		return time.Unix(v, 0)
	case int:
		if int64(v) > 1e12 {
			return time.UnixMilli(int64(v))
		}
		return time.Unix(int64(v), 0)
	}
	return time.Time{}
}

// parseTraceID converts hex string to TraceID
func parseTraceID(s string) (pcommon.TraceID, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return pcommon.TraceID{}, err
	}
	if len(b) != 16 {
		return pcommon.TraceID{}, errors.New("trace_id must be 16 bytes")
	}
	var tid pcommon.TraceID
	copy(tid[:], b)
	return tid, nil
}

// parseSpanID converts hex string to SpanID
func parseSpanID(s string) (pcommon.SpanID, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return pcommon.SpanID{}, err
	}
	if len(b) != 8 {
		return pcommon.SpanID{}, errors.New("span_id must be 8 bytes")
	}
	var sid pcommon.SpanID
	copy(sid[:], b)
	return sid, nil
}

// insertValueToAttributes adds a value to pcommon.Map
func insertValueToAttributes(attrs pcommon.Map, key string, value any) {
	switch v := value.(type) {
	case string:
		attrs.PutStr(key, v)
	case float64:
		attrs.PutDouble(key, v)
	case int64:
		attrs.PutInt(key, v)
	case int:
		attrs.PutInt(key, int64(v))
	case bool:
		attrs.PutBool(key, v)
	case map[string]any:
		nested := attrs.PutEmptyMap(key)
		insertMapToAttributes(v, nested)
	case []any:
		slice := attrs.PutEmptySlice(key)
		for _, item := range v {
			insertValueToSlice(slice, item)
		}
	default:
		attrs.PutStr(key, fmt.Sprintf("%v", v))
	}
}

func insertMapToAttributes(m map[string]any, attrs pcommon.Map) {
	for k, v := range m {
		insertValueToAttributes(attrs, k, v)
	}
}

func insertValueToSlice(slice pcommon.Slice, value any) {
	switch v := value.(type) {
	case string:
		slice.AppendEmpty().SetStr(v)
	case float64:
		slice.AppendEmpty().SetDouble(v)
	case int64:
		slice.AppendEmpty().SetInt(v)
	case int:
		slice.AppendEmpty().SetInt(int64(v))
	case bool:
		slice.AppendEmpty().SetBool(v)
	case map[string]any:
		nested := slice.AppendEmpty().SetEmptyMap()
		insertMapToAttributes(v, nested)
	default:
		slice.AppendEmpty().SetStr(fmt.Sprintf("%v", v))
	}
}
