// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package s3receiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/s3receiver"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.uber.org/multierr"
)

var (
	errMissingEndpointFromConfig   = errors.New("missing receiver server endpoint from config")
	errInvalidParseFormat          = errors.New("parse_format must be 'lines', 'json', or 'auto'")
	errNegativeMaxObjectSize       = errors.New("max_object_size cannot be negative")
	errReadTimeoutExceedsMaxValue  = errors.New("the duration specified for read_timeout exceeds the maximum allowed value of 60s")
	errWriteTimeoutExceedsMaxValue = errors.New("the duration specified for write_timeout exceeds the maximum allowed value of 60s")
)

// Config defines configuration for the S3 receiver.
type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct

	// ReadTimeout is the wait time for reading request headers. Default is 30s.
	ReadTimeout time.Duration `mapstructure:"read_timeout"`

	// WriteTimeout is the wait time for writing request response. Default is 30s.
	WriteTimeout time.Duration `mapstructure:"write_timeout"`

	// BucketName restricts uploads to a specific bucket (empty = accept all)
	BucketName string `mapstructure:"bucket_name"`

	// ParseFormat specifies how to interpret object bodies: "lines", "json", or "auto"
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
