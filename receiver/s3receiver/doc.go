// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package s3receiver provides an S3-compatible endpoint that captures object uploads
// and converts them into OTLP log events. It uses gofakes3 for S3 API compatibility.
package s3receiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/s3receiver"
