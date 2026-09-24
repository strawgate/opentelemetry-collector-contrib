// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package secretstore // import "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/secretstore"

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
)

const (
	keySize = 32

	defaultEnvKeyName        = "OPAMP_SUPERVISOR_SECRETS_KEY"
	defaultCredentialKeyName = "opamp-supervisor-secrets-key"
	defaultKeychainService   = "io.opentelemetry.opampsupervisor"
	defaultKeychainPath      = "/Library/Keychains/System.keychain"
	defaultDPAPIKeyFile      = "secrets.key.dpapi" //nolint:gosec // G101: a file name, not a credential
)

// keySource provides the 32-byte key that encrypts persisted secrets.
type keySource interface {
	key() ([]byte, error)
}

func newKeySource(dir string, cfg config.Secrets) (keySource, error) {
	switch cfg.KeySource {
	case config.SecretsKeySourceEnv:
		return envKeySource{name: orDefault(cfg.KeyName, defaultEnvKeyName)}, nil
	case config.SecretsKeySourceSystemdCredential:
		return credentialKeySource{name: orDefault(cfg.KeyName, defaultCredentialKeyName)}, nil
	case config.SecretsKeySourceKeychain:
		return newKeychainKeySource(orDefault(cfg.KeyName, defaultKeychainService), orDefault(cfg.KeychainPath, defaultKeychainPath))
	case config.SecretsKeySourceDPAPI:
		return newDPAPIKeySource(filepath.Join(dir, orDefault(cfg.KeyName, defaultDPAPIKeyFile)))
	default:
		return nil, fmt.Errorf("unknown secrets key source %q", cfg.KeySource)
	}
}

// envKeySource reads a base64-encoded key from an environment variable.
type envKeySource struct {
	name string
}

func (e envKeySource) key() ([]byte, error) {
	value, ok := os.LookupEnv(e.name)
	if !ok {
		return nil, fmt.Errorf("environment variable %s is not set", e.name)
	}
	return decodeKey([]byte(value))
}

// credentialKeySource reads the key from a systemd credential, which systemd
// decrypts into $CREDENTIALS_DIRECTORY for the service.
type credentialKeySource struct {
	name string
}

func (c credentialKeySource) key() ([]byte, error) {
	dir, ok := os.LookupEnv("CREDENTIALS_DIRECTORY")
	if !ok {
		return nil, errors.New("CREDENTIALS_DIRECTORY is not set; provide the key with LoadCredential= or LoadCredentialEncrypted=")
	}
	data, err := os.ReadFile(filepath.Join(dir, c.name))
	if err != nil {
		return nil, err
	}
	return decodeKey(data)
}

// decodeKey accepts a raw 32-byte key or its standard base64 encoding.
func decodeKey(data []byte) ([]byte, error) {
	if len(data) == keySize {
		return data, nil
	}
	trimmed := bytes.TrimSpace(data)
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(trimmed)))
	n, err := base64.StdEncoding.Decode(decoded, trimmed)
	if err != nil || n != keySize {
		return nil, fmt.Errorf("the key must be %d bytes, raw or base64-encoded", keySize)
	}
	return decoded[:n], nil
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
