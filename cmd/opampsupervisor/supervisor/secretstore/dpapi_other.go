// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package secretstore // import "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/secretstore"

import "errors"

func newDPAPIKeySource(string) (keySource, error) {
	return nil, errors.New("the dpapi key source is only supported on Windows")
}
