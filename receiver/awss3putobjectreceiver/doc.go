// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package awss3putobjectreceiver provides an S3-compatible endpoint that captures
// PutObject requests and converts them into OTLP log events. It uses gofakes3 for
// S3 API compatibility and can be used to receive logs from any S3-compatible client.
package awss3putobjectreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3putobjectreceiver"
