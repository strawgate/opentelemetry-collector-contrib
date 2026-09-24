// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package secretstore

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
)

func testKey(fill byte) string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{fill}, keySize))
}

func TestFileStore(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir, config.Secrets{})
	require.NoError(t, err)

	_, err = store.Read("missing.dat")
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, store.Remove("missing.dat"))

	require.NoError(t, store.Write("secret.dat", []byte("first")))
	require.NoError(t, store.Write("secret.dat", []byte("second")))
	got, err := store.Read("secret.dat")
	require.NoError(t, err)
	assert.Equal(t, []byte("second"), got)

	if runtime.GOOS != "windows" {
		info, statErr := os.Stat(filepath.Join(dir, "secret.dat"))
		require.NoError(t, statErr)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "no temporary files are left behind")

	require.NoError(t, store.Remove("secret.dat"))
	_, err = store.Read("secret.dat")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestEncryptedStore(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TEST_SECRETS_KEY", testKey(1))
	store, err := New(dir, config.Secrets{KeySource: config.SecretsKeySourceEnv, KeyName: "TEST_SECRETS_KEY"})
	require.NoError(t, err)

	require.NoError(t, store.Write("secret.dat", []byte("bearer token")))
	raw, err := os.ReadFile(filepath.Join(dir, "secret.dat"))
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "bearer token")
	assert.True(t, bytes.HasPrefix(raw, encryptedPrefix))

	got, err := store.Read("secret.dat")
	require.NoError(t, err)
	assert.Equal(t, []byte("bearer token"), got)

	t.Run("the name is authenticated", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "other.dat"), raw, 0o600))
		_, err := store.Read("other.dat")
		require.ErrorContains(t, err, "decrypt other.dat")
	})

	t.Run("a different key cannot read it", func(t *testing.T) {
		t.Setenv("TEST_SECRETS_KEY", testKey(2))
		other, err := New(dir, config.Secrets{KeySource: config.SecretsKeySourceEnv, KeyName: "TEST_SECRETS_KEY"})
		require.NoError(t, err)
		_, err = other.Read("secret.dat")
		require.ErrorContains(t, err, "decrypt secret.dat")
	})

	t.Run("an unencrypted store refuses encrypted files", func(t *testing.T) {
		plain, err := New(dir, config.Secrets{})
		require.NoError(t, err)
		_, err = plain.Read("secret.dat")
		require.ErrorContains(t, err, "is encrypted")
	})

	t.Run("files written before encryption are encrypted on read", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "legacy.dat"), []byte("old token"), 0o600))
		got, err := store.Read("legacy.dat")
		require.NoError(t, err)
		assert.Equal(t, []byte("old token"), got)
		raw, err := os.ReadFile(filepath.Join(dir, "legacy.dat"))
		require.NoError(t, err)
		assert.True(t, bytes.HasPrefix(raw, encryptedPrefix))
	})

	t.Run("truncated files are rejected", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "short.dat"), append(append([]byte{}, encryptedPrefix...), 1, 2), 0o600))
		_, err := store.Read("short.dat")
		require.ErrorContains(t, err, "truncated")
	})
}

func TestEnvKeySource(t *testing.T) {
	_, err := New(t.TempDir(), config.Secrets{KeySource: config.SecretsKeySourceEnv, KeyName: "TEST_SECRETS_KEY_UNSET"})
	require.ErrorContains(t, err, "TEST_SECRETS_KEY_UNSET is not set")

	t.Setenv("TEST_SECRETS_KEY_SHORT", base64.StdEncoding.EncodeToString([]byte("too short")))
	_, err = New(t.TempDir(), config.Secrets{KeySource: config.SecretsKeySourceEnv, KeyName: "TEST_SECRETS_KEY_SHORT"})
	require.ErrorContains(t, err, "must be 32 bytes")

	t.Setenv(defaultEnvKeyName, testKey(3))
	_, err = New(t.TempDir(), config.Secrets{KeySource: config.SecretsKeySourceEnv})
	require.NoError(t, err, "the default variable name is used")
}

func TestCredentialKeySource(t *testing.T) {
	credentials := t.TempDir()
	t.Setenv("CREDENTIALS_DIRECTORY", credentials)
	cfg := config.Secrets{KeySource: config.SecretsKeySourceSystemdCredential}

	_, err := New(t.TempDir(), cfg)
	require.ErrorIs(t, err, os.ErrNotExist)

	// systemd-creds writes the credential as raw bytes.
	require.NoError(t, os.WriteFile(filepath.Join(credentials, defaultCredentialKeyName), bytes.Repeat([]byte{4}, keySize), 0o600))
	dir := t.TempDir()
	store, err := New(dir, cfg)
	require.NoError(t, err)
	require.NoError(t, store.Write("secret.dat", []byte("token")))

	// A base64-encoded credential holding the same key reads it back.
	require.NoError(t, os.WriteFile(filepath.Join(credentials, "encoded"), []byte(testKey(4)+"\n"), 0o600))
	encoded, err := New(dir, config.Secrets{KeySource: config.SecretsKeySourceSystemdCredential, KeyName: "encoded"})
	require.NoError(t, err)
	got, err := encoded.Read("secret.dat")
	require.NoError(t, err)
	assert.Equal(t, []byte("token"), got)
}

func TestCredentialKeySourceRequiresDirectory(t *testing.T) {
	t.Setenv("CREDENTIALS_DIRECTORY", "")
	require.NoError(t, os.Unsetenv("CREDENTIALS_DIRECTORY"))
	_, err := New(t.TempDir(), config.Secrets{KeySource: config.SecretsKeySourceSystemdCredential})
	require.ErrorContains(t, err, "CREDENTIALS_DIRECTORY is not set")
}

func TestUnsupportedKeySources(t *testing.T) {
	if runtime.GOOS != "darwin" {
		_, err := New(t.TempDir(), config.Secrets{KeySource: config.SecretsKeySourceKeychain})
		require.ErrorContains(t, err, "only supported on macOS")
	}
	if runtime.GOOS != "windows" {
		_, err := New(t.TempDir(), config.Secrets{KeySource: config.SecretsKeySourceDPAPI})
		require.ErrorContains(t, err, "only supported on Windows")
	}
	_, err := New(t.TempDir(), config.Secrets{KeySource: "vault"})
	require.ErrorContains(t, err, `unknown secrets key source "vault"`)
}
