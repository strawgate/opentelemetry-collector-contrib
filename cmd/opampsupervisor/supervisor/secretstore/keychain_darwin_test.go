// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build darwin

package secretstore

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
)

// newTestKeychain creates an unlocked keychain in a temporary directory and
// restores the user's keychain search list afterwards.
func newTestKeychain(t *testing.T) string {
	t.Helper()
	searchList, err := exec.Command(securityCommand, "list-keychains", "-d", "user").Output()
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "test.keychain-db")
	require.NoError(t, exec.Command(securityCommand, "create-keychain", "-p", "test", path).Run())
	t.Cleanup(func() {
		_ = exec.Command(securityCommand, "delete-keychain", path).Run()
		args := []string{"list-keychains", "-d", "user", "-s"}
		for line := range strings.SplitSeq(string(searchList), "\n") {
			if kc := strings.Trim(strings.TrimSpace(line), `"`); kc != "" {
				args = append(args, kc)
			}
		}
		_ = exec.Command(securityCommand, args...).Run()
	})
	require.NoError(t, exec.Command(securityCommand, "unlock-keychain", "-p", "test", path).Run())
	return path
}

func TestKeychainKeySource(t *testing.T) {
	keychain := newTestKeychain(t)
	cfg := config.Secrets{KeySource: config.SecretsKeySourceKeychain, KeychainPath: keychain, KeyName: "io.opentelemetry.opampsupervisor.test"}

	dir := t.TempDir()
	store, err := New(dir, cfg)
	require.NoError(t, err, "the key is created on first use")
	require.NoError(t, store.Write("secret.dat", []byte("token")))

	again, err := New(dir, cfg)
	require.NoError(t, err)
	got, err := again.Read("secret.dat")
	require.NoError(t, err, "a second store reads the same key back from the keychain")
	assert.Equal(t, []byte("token"), got)

	out, err := exec.Command(securityCommand, "find-generic-password", "-s", cfg.KeyName, "-a", keychainAccount, keychain).CombinedOutput() //nolint:gosec // G204: test keychain
	require.NoError(t, err, string(out))
}

func TestKeychainKeySourceMissingKeychain(t *testing.T) {
	service := "io.opentelemetry.opampsupervisor.test-missing"
	missing := filepath.Join(t.TempDir(), "missing.keychain-db")
	cfg := config.Secrets{KeySource: config.SecretsKeySourceKeychain, KeychainPath: missing, KeyName: service}

	_, err := New(t.TempDir(), cfg)
	require.ErrorContains(t, err, missing)

	// security(1) would otherwise have fallen back to the default keychain.
	err = exec.Command(securityCommand, "find-generic-password", "-s", service, "-a", keychainAccount).Run()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "no key is written to the default keychain")
	assert.Equal(t, securityItemNotFound, exitErr.ExitCode())
}
