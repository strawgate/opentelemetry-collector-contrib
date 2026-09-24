// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !darwin

package secretstore // import "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/secretstore"

import "errors"

func newKeychainKeySource(string, string) (keySource, error) {
	return nil, errors.New("the keychain key source is only supported on macOS")
}
