// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build darwin

package secretstore // import "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/secretstore"

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const (
	securityCommand = "/usr/bin/security"
	keychainAccount = "secrets-key"
	// errSecItemNotFound, returned by security(1) when there is no such item.
	securityItemNotFound = 44
)

// keychainKeySource keeps the key in a macOS keychain through security(1),
// which works without cgo. The item is created on first use. A LaunchDaemon
// has no login keychain, so the default is the System keychain, which
// requires running as root.
type keychainKeySource struct {
	service string
	path    string
}

func newKeychainKeySource(service, path string) (keySource, error) {
	return keychainKeySource{service: service, path: path}, nil
}

func (k keychainKeySource) key() ([]byte, error) {
	// security(1) silently uses the default keychain when the named one does
	// not exist, which would put the key somewhere nobody configured.
	if _, err := os.Stat(k.path); err != nil {
		return nil, fmt.Errorf("keychain %s: %w", k.path, err)
	}

	key, err := k.find()
	if err == nil {
		return key, nil
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != securityItemNotFound {
		return nil, err
	}

	key, err = newRandomKey()
	if err != nil {
		return nil, err
	}
	if storeErr := k.store(key); storeErr != nil {
		return nil, storeErr
	}
	// Read it back so a key that was not stored where expected is caught now,
	// not after a restart when everything encrypted with it is unreadable.
	stored, err := k.find()
	if err != nil {
		return nil, fmt.Errorf("read back the key stored in %s: %w", k.path, err)
	}
	if !bytes.Equal(stored, key) {
		return nil, fmt.Errorf("the key read back from %s does not match the key stored", k.path)
	}
	return key, nil
}

func (k keychainKeySource) find() ([]byte, error) {
	var stderr bytes.Buffer
	//nolint:gosec // G204: a fixed binary; the service and keychain path come from the Supervisor's config
	cmd := exec.Command(securityCommand, "find-generic-password", "-s", k.service, "-a", keychainAccount, "-w", k.path)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("read the key from %s: %w: %s", k.path, err, strings.TrimSpace(stderr.String()))
	}
	return decodeKey(bytes.TrimSpace(out))
}

// store adds the key through security's interactive mode, so the key is
// passed on stdin rather than on a command line other users can see.
func (k keychainKeySource) store(key []byte) error {
	command := fmt.Sprintf("add-generic-password -s %s -a %s -w %s %s\n",
		shellQuote(k.service), shellQuote(keychainAccount),
		shellQuote(base64.StdEncoding.EncodeToString(key)), shellQuote(k.path))
	var stderr bytes.Buffer
	cmd := exec.Command(securityCommand, "-i")
	cmd.Stdin = strings.NewReader(command)
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("store the key in %s: %w: %s", k.path, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// shellQuote quotes s for security's interactive mode, which splits commands
// like a shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
