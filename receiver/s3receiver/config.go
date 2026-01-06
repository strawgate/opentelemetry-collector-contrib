// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package s3receiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/s3receiver"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.uber.org/multierr"
)

var (
	errMissingEndpointFromConfig   = errors.New("missing receiver server endpoint from config")
	errInvalidParseFormat          = errors.New("parse_format must be 'lines', 'json', or 'auto'")
	errNegativeMaxObjectSize       = errors.New("max_object_size cannot be negative")
	errReadTimeoutExceedsMaxValue  = errors.New("the duration specified for read_timeout exceeds the maximum allowed value of 60s")
	errWriteTimeoutExceedsMaxValue = errors.New("the duration specified for write_timeout exceeds the maximum allowed value of 60s")
	errEncodingAndParseFormat      = errors.New("encoding and parse_format cannot be used together")
)

// Encoding defines the encoding extension configuration for parsing uploaded objects.
type Encoding struct {
	// Extension is the component ID of the encoding extension to use for unmarshaling logs.
	// Supported extensions include:
	// - encoding/json_log: For JSON and NDJSON log formats
	// - encoding/awslogs: For AWS log formats (CloudWatch, VPC Flow Logs, S3 Access Logs, etc.)
	Extension component.ID `mapstructure:"extension"`

	// Suffix is an optional file suffix filter. Only objects with keys ending in this suffix
	// will be processed by the encoding extension. Leave empty to process all objects.
	Suffix string `mapstructure:"suffix"`
}

// Config defines configuration for the S3 receiver.
type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct

	// ReadTimeout is the wait time for reading request headers. Default is 30s.
	ReadTimeout time.Duration `mapstructure:"read_timeout"`

	// WriteTimeout is the wait time for writing request response. Default is 30s.
	WriteTimeout time.Duration `mapstructure:"write_timeout"`

	// BucketName restricts uploads to a specific bucket (empty = accept all)
	BucketName string `mapstructure:"bucket_name"`

	// Encoding specifies an encoding extension to use for parsing uploaded objects.
	// When set, this takes precedence over parse_format.
	Encoding *Encoding `mapstructure:"encoding"`

	// ParseFormat specifies how to interpret object bodies: "lines", "json", or "auto".
	// This is ignored if Encoding is set.
	// Deprecated: Use encoding extension instead for better format support.
	ParseFormat string `mapstructure:"parse_format"`

	// AutoDecompress enables automatic gzip detection and decompression
	AutoDecompress bool `mapstructure:"auto_decompress"`

	// MaxObjectSize limits the maximum size of accepted objects (bytes)
	MaxObjectSize int64 `mapstructure:"max_object_size"`

	// ResourceAttributes are added to all emitted logs
	ResourceAttributes map[string]string `mapstructure:"resource_attributes"`
}

func (cfg *Config) Validate() error {
	var errs error

	maxReadWriteTimeout := 60 * time.Second

	if cfg.Endpoint == "" {
		errs = multierr.Append(errs, errMissingEndpointFromConfig)
	}

	// Encoding and parse_format are mutually exclusive
	if cfg.Encoding != nil && cfg.ParseFormat != "" {
		errs = multierr.Append(errs, errEncodingAndParseFormat)
	}

	if cfg.ParseFormat != "" && cfg.ParseFormat != "lines" &&
		cfg.ParseFormat != "json" && cfg.ParseFormat != "auto" {
		errs = multierr.Append(errs, errInvalidParseFormat)
	}

	if cfg.MaxObjectSize < 0 {
		errs = multierr.Append(errs, errNegativeMaxObjectSize)
	}

	if cfg.ReadTimeout > maxReadWriteTimeout {
		errs = multierr.Append(errs, errReadTimeoutExceedsMaxValue)
	}

	if cfg.WriteTimeout > maxReadWriteTimeout {
		errs = multierr.Append(errs, errWriteTimeoutExceedsMaxValue)
	}

	return errs
}
