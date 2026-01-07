#!/bin/bash
# Starts a minimal OpenTelemetry Collector with the awss3putobject receiver
# Usage: ./start_collector.sh [port]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RECEIVER_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$RECEIVER_DIR/../.." && pwd)"
COLLECTOR_PORT="${1:-9000}"
BUILD_DIR="${SCRIPT_DIR}/.collector-build"

echo "=== AWS S3 PutObject Receiver - Collector Startup ==="
echo "Receiver directory: $RECEIVER_DIR"
echo "Port: $COLLECTOR_PORT"
echo ""

# Create build directory
mkdir -p "$BUILD_DIR"

# Step 1: Create collector build configuration
echo "Step 1: Creating collector build configuration..."

cat > "$BUILD_DIR/builder-config.yaml" << EOF
dist:
  name: otelcol-awss3putobject
  description: Collector with AWS S3 PutObject receiver
  output_path: $BUILD_DIR/collector

receivers:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3putobjectreceiver v0.0.0

exporters:
  - gomod: go.opentelemetry.io/collector/exporter/debugexporter v0.120.0

extensions:
  - gomod: go.opentelemetry.io/collector/extension/zpagesextension v0.120.0

processors:
  - gomod: go.opentelemetry.io/collector/processor/batchprocessor v0.120.0

replaces:
  - github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3putobjectreceiver => $RECEIVER_DIR
EOF

# Step 2: Create collector configuration
echo "Step 2: Creating collector configuration..."

cat > "$BUILD_DIR/collector-config.yaml" << EOF
receivers:
  awss3putobject:
    endpoint: "localhost:$COLLECTOR_PORT"
    parse_format: "json"
    auto_decompress: true
    max_object_size: 10485760
    read_timeout: 30s
    write_timeout: 30s
    resource_attributes:
      receiver.type: "awss3putobject"

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    logs:
      receivers: [awss3putobject]
      exporters: [debug]
EOF

# Step 3: Build the collector
echo "Step 3: Building collector (this may take a minute)..."

cd "$BUILD_DIR"

# Check if ocb/builder is available
if command -v ocb &> /dev/null; then
    OCB_CMD="ocb"
elif command -v builder &> /dev/null; then
    OCB_CMD="builder"
elif [ -f "$HOME/go/bin/builder" ]; then
    OCB_CMD="$HOME/go/bin/builder"
else
    echo "Installing OpenTelemetry Collector Builder..."
    go install go.opentelemetry.io/collector/cmd/builder@v0.120.0
    OCB_CMD="$HOME/go/bin/builder"
fi

echo "Using builder: $OCB_CMD"

# Only rebuild if binary doesn't exist or source changed
if [ ! -f "$BUILD_DIR/collector/otelcol-awss3putobject" ] || \
   [ "$RECEIVER_DIR/receiver.go" -nt "$BUILD_DIR/collector/otelcol-awss3putobject" ]; then
    $OCB_CMD --config builder-config.yaml
else
    echo "Using cached collector build"
fi

# Step 4: Start the collector
echo ""
echo "Step 4: Starting collector on port $COLLECTOR_PORT..."
echo "=========================================="
echo ""

exec "$BUILD_DIR/collector/otelcol-awss3putobject" --config "$BUILD_DIR/collector-config.yaml"
