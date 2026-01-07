#!/bin/bash
# Sends test data to the awss3putobject receiver using the AWS CLI
# Usage: ./start_sender.sh [endpoint_url]
#
# Prerequisites:
#   - AWS CLI v2 installed (brew install awscli or apt install awscli)
#   - Collector running with awss3putobject receiver (use start_collector.sh)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENDPOINT_URL="${1:-http://localhost:9000}"
BUCKET_NAME="test-logs"
TEST_DATA_DIR="${SCRIPT_DIR}/.test-data"

echo "=== AWS S3 PutObject Receiver - Test Sender ==="
echo "Endpoint: $ENDPOINT_URL"
echo "Bucket: $BUCKET_NAME"
echo ""

# Create test data directory
mkdir -p "$TEST_DATA_DIR"

# Check if AWS CLI is available
if ! command -v aws &> /dev/null; then
    echo "ERROR: AWS CLI is not installed"
    echo ""
    echo "Install it with:"
    echo "  macOS:  brew install awscli"
    echo "  Ubuntu: sudo apt install awscli"
    echo "  or:     pip install awscli"
    exit 1
fi

# Configure AWS CLI for local S3-compatible endpoint
# Using dummy credentials since our receiver doesn't require auth
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="us-east-1"

# Common AWS CLI options for S3-compatible endpoint
AWS_OPTS="--endpoint-url $ENDPOINT_URL --no-sign-request"

echo "Testing connection to $ENDPOINT_URL..."
if ! curl -s "$ENDPOINT_URL" > /dev/null 2>&1; then
    echo "ERROR: Cannot connect to $ENDPOINT_URL"
    echo "Make sure the collector is running (./start_collector.sh)"
    exit 1
fi
echo "Connection OK!"
echo ""

# ============================================================================
# Test 1: JSON Log Lines
# ============================================================================
echo "Test 1: Sending JSON log lines..."

cat > "$TEST_DATA_DIR/app-logs.json" << 'EOF'
{"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"Application started","version":"1.2.3"}
{"timestamp":"2024-01-15T10:30:01Z","level":"DEBUG","message":"Loading configuration","config_path":"/etc/app/config.yaml"}
{"timestamp":"2024-01-15T10:30:02Z","level":"INFO","message":"Connected to database","host":"db.example.com","latency_ms":42}
{"timestamp":"2024-01-15T10:30:05Z","level":"WARN","message":"High memory usage detected","memory_percent":85}
{"timestamp":"2024-01-15T10:30:10Z","level":"ERROR","message":"Failed to process request","error":"connection timeout","request_id":"req-123"}
EOF

aws s3 cp "$TEST_DATA_DIR/app-logs.json" "s3://$BUCKET_NAME/app/server.json" \
    $AWS_OPTS \
    --metadata "source=e2e-test,application=demo-app,environment=testing"

echo "  ✓ Sent 5 JSON log records to s3://$BUCKET_NAME/app/server.json"

# ============================================================================
# Test 2: CloudWatch-style logs (JSON with @timestamp)
# ============================================================================
echo ""
echo "Test 2: Sending CloudWatch-style logs..."

cat > "$TEST_DATA_DIR/cloudwatch-logs.json" << 'EOF'
{"@timestamp":"2024-01-15T10:31:00.000Z","@message":"Lambda function invoked","requestId":"abc-123","functionName":"myFunction"}
{"@timestamp":"2024-01-15T10:31:00.500Z","@message":"Processing event","eventType":"S3","bucket":"source-bucket"}
{"@timestamp":"2024-01-15T10:31:01.200Z","@message":"Function completed successfully","duration":1200,"billedDuration":1300}
EOF

aws s3 cp "$TEST_DATA_DIR/cloudwatch-logs.json" "s3://$BUCKET_NAME/cloudwatch/lambda.json" \
    $AWS_OPTS \
    --metadata "log-group=/aws/lambda/myFunction,log-stream=2024/01/15"

echo "  ✓ Sent 3 CloudWatch-style log records"

# ============================================================================
# Test 3: Gzip compressed logs
# ============================================================================
echo ""
echo "Test 3: Sending gzip compressed logs..."

cat > "$TEST_DATA_DIR/compressed-logs.json" << 'EOF'
{"level":"INFO","message":"Compressed log entry 1"}
{"level":"DEBUG","message":"Compressed log entry 2"}
{"level":"WARN","message":"Compressed log entry 3"}
EOF

gzip -c "$TEST_DATA_DIR/compressed-logs.json" > "$TEST_DATA_DIR/compressed-logs.json.gz"

aws s3 cp "$TEST_DATA_DIR/compressed-logs.json.gz" "s3://$BUCKET_NAME/compressed/logs.json.gz" \
    $AWS_OPTS \
    --content-encoding gzip

echo "  ✓ Sent 3 gzip-compressed log records"

# ============================================================================
# Test 4: Multi-line JSON (e.g., from an application with nested data)
# ============================================================================
echo ""
echo "Test 4: Sending logs with nested JSON..."

cat > "$TEST_DATA_DIR/nested-logs.json" << 'EOF'
{"level":"INFO","message":"User action","user":{"id":"user-456","name":"John Doe"},"action":"login"}
{"level":"ERROR","message":"API error","error":{"code":"E001","message":"Rate limit exceeded","details":{"limit":100,"current":150}}}
{"level":"DEBUG","message":"Cache stats","cache":{"hits":1000,"misses":50,"size_mb":256}}
EOF

aws s3 cp "$TEST_DATA_DIR/nested-logs.json" "s3://$BUCKET_NAME/app/nested.json" \
    $AWS_OPTS

echo "  ✓ Sent 3 nested JSON log records"

# ============================================================================
# Test 5: Using s3api for more control (with custom metadata)
# ============================================================================
echo ""
echo "Test 5: Using s3api with custom metadata..."

cat > "$TEST_DATA_DIR/api-logs.json" << 'EOF'
{"level":"INFO","message":"API request","method":"GET","path":"/api/v1/users","status":200}
{"level":"INFO","message":"API request","method":"POST","path":"/api/v1/orders","status":201}
EOF

aws s3api put-object \
    --bucket "$BUCKET_NAME" \
    --key "api/requests.json" \
    --body "$TEST_DATA_DIR/api-logs.json" \
    --content-type "application/json" \
    --metadata '{"source":"api-gateway","version":"v1","region":"us-east-1"}' \
    $AWS_OPTS

echo "  ✓ Sent 2 API log records with s3api"

# ============================================================================
# Test 6: List uploaded objects
# ============================================================================
echo ""
echo "Test 6: Listing uploaded objects..."
echo ""

aws s3 ls "s3://$BUCKET_NAME/" --recursive $AWS_OPTS 2>/dev/null || echo "(Listing may not be supported)"

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo "Total log records sent: 16"
echo ""
echo "Check the collector output for received logs."
echo "The collector should show LogRecord entries with:"
echo "  - Parsed JSON fields as attributes"
echo "  - Severity levels (INFO, DEBUG, WARN, ERROR)"
echo "  - Timestamps from the JSON data"
echo "  - S3 metadata as resource attributes"
echo ""

# Cleanup
rm -rf "$TEST_DATA_DIR"

echo "Done!"
