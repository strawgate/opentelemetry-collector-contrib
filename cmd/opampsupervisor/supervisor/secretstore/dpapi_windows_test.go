// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package secretstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
)

func TestDPAPIKeySource(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Secrets{KeySource: config.SecretsKeySourceDPAPI}

	store, err := New(dir, cfg)
	require.NoError(t, err, "the key is created on first use")
	require.NoError(t, store.Write("secret.dat", []byte("token")))

	protected, err := os.ReadFile(filepath.Join(dir, defaultDPAPIKeyFile))
	require.NoError(t, err)
	assert.NotEqual(t, keySize, len(protected), "the key file holds DPAPI output, not the raw key")

	again, err := New(dir, cfg)
	require.NoError(t, err)
	got, err := again.Read("secret.dat")
	require.NoError(t, err, "a second store unprotects the same key")
	assert.Equal(t, []byte("token"), got)
}
