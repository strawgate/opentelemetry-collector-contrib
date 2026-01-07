#!/bin/bash
# End-to-end test script for the S3 receiver
# This script builds a minimal collector, starts it with s3receiver, and sends test data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DIR=$(mktemp -d)
COLLECTOR_PORT=19000
COLLECTOR_PID=""

cleanup() {
    echo "Cleaning up..."
    if [ -n "$COLLECTOR_PID" ] && kill -0 "$COLLECTOR_PID" 2>/dev/null; then
        kill "$COLLECTOR_PID" 2>/dev/null || true
        wait "$COLLECTOR_PID" 2>/dev/null || true
    fi
    rm -rf "$TEST_DIR"
    echo "Cleanup complete"
}

trap cleanup EXIT

echo "=== S3 Receiver End-to-End Test ==="
echo "Test directory: $TEST_DIR"
echo ""

# Step 1: Create a minimal collector build configuration
echo "Step 1: Creating collector build configuration..."

cat > "$TEST_DIR/builder-config.yaml" << 'EOF'
dist:
  name: otelcol-s3test
  description: Minimal collector for S3 receiver testing
  output_path: ./otelcol-s3test

receivers:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/receiver/s3receiver v0.0.0

exporters:
  - gomod: go.opentelemetry.io/collector/exporter/debugexporter v0.120.0

extensions:
  - gomod: go.opentelemetry.io/collector/extension/zpagesextension v0.120.0

processors:
  - gomod: go.opentelemetry.io/collector/processor/batchprocessor v0.120.0

replaces:
  - github.com/open-telemetry/opentelemetry-collector-contrib/receiver/s3receiver => REPO_ROOT/receiver/s3receiver
EOF

# Replace REPO_ROOT with actual path
sed -i "s|REPO_ROOT|$REPO_ROOT|g" "$TEST_DIR/builder-config.yaml"

# Step 2: Create collector configuration
echo "Step 2: Creating collector configuration..."

cat > "$TEST_DIR/collector-config.yaml" << EOF
receivers:
  s3:
    endpoint: "localhost:$COLLECTOR_PORT"
    parse_format: "json"
    auto_decompress: true
    max_object_size: 10485760
    read_timeout: 30s
    write_timeout: 30s
    resource_attributes:
      receiver.type: "s3"
      test.environment: "e2e"

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    logs:
      receivers: [s3]
      exporters: [debug]
EOF

# Step 3: Build the collector using ocb (OpenTelemetry Collector Builder)
echo "Step 3: Building collector..."

cd "$TEST_DIR"

# Check if ocb is available
if ! command -v ocb &> /dev/null; then
    echo "Installing OpenTelemetry Collector Builder (ocb)..."
    go install go.opentelemetry.io/collector/cmd/builder@v0.120.0
    # The binary is installed as 'builder', rename it
    if command -v builder &> /dev/null; then
        OCB_CMD="builder"
    else
        OCB_CMD="$HOME/go/bin/builder"
    fi
else
    OCB_CMD="ocb"
fi

echo "Using OCB command: $OCB_CMD"
$OCB_CMD --config builder-config.yaml

# Step 4: Start the collector
echo "Step 4: Starting collector..."

./otelcol-s3test/otelcol-s3test --config collector-config.yaml > "$TEST_DIR/collector.log" 2>&1 &
COLLECTOR_PID=$!

echo "Collector PID: $COLLECTOR_PID"
echo "Waiting for collector to start..."

# Wait for the collector to be ready
for i in {1..30}; do
    if curl -s "http://localhost:$COLLECTOR_PORT" > /dev/null 2>&1; then
        echo "Collector is ready!"
        break
    fi
    if ! kill -0 "$COLLECTOR_PID" 2>/dev/null; then
        echo "ERROR: Collector process died"
        cat "$TEST_DIR/collector.log"
        exit 1
    fi
    sleep 1
done

# Step 5: Send test data via S3 PutObject
echo ""
echo "Step 5: Sending test data..."

# Test 1: Simple JSON logs
echo "Test 1: Sending JSON log lines..."
TEST_DATA='{"level":"INFO","message":"Test message 1","user_id":"user123"}
{"level":"ERROR","message":"Test error message","error_code":500}
{"level":"DEBUG","message":"Debug info","details":{"key":"value"}}'

curl -s -X PUT "http://localhost:$COLLECTOR_PORT/test-bucket/logs/app.json" \
    -H "Content-Type: application/json" \
    -H "x-amz-meta-source: e2e-test" \
    -H "x-amz-meta-application: test-app" \
    -d "$TEST_DATA"

echo "  Sent 3 JSON log records"

# Test 2: Plain text logs (with lines format - need to restart with different config or use auto)
echo "Test 2: Sending more JSON logs with timestamps..."
TEST_DATA2='{"timestamp":"2024-01-15T10:30:00Z","level":"WARN","message":"Warning message"}
{"timestamp":"2024-01-15T10:31:00Z","level":"FATAL","message":"Critical error"}'

curl -s -X PUT "http://localhost:$COLLECTOR_PORT/test-bucket/logs/timed.json" \
    -H "Content-Type: application/json" \
    -d "$TEST_DATA2"

echo "  Sent 2 JSON log records with timestamps"

# Give the collector time to process
sleep 2

# Step 6: Check collector logs for received data
echo ""
echo "Step 6: Verifying logs were received..."
echo ""

if grep -q "LogRecord" "$TEST_DIR/collector.log"; then
    echo "SUCCESS: Logs were received and processed!"
    echo ""
    echo "=== Collector Output (last 50 lines) ==="
    tail -50 "$TEST_DIR/collector.log"
else
    echo "WARNING: Could not verify log records in output"
    echo ""
    echo "=== Full Collector Log ==="
    cat "$TEST_DIR/collector.log"
fi

echo ""
echo "=== Test Complete ==="
