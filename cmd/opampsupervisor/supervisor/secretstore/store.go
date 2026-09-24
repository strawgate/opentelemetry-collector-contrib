// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package secretstore persists small secrets in the Supervisor's storage
// directory, optionally encrypted with a key held by the operating system.
package secretstore // import "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/secretstore"

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
)

// Store reads and writes named secrets. A name is a file name within the
// storage directory.
type Store interface {
	// Read returns the secret, or an error wrapping os.ErrNotExist if there
	// is none.
	Read(name string) ([]byte, error)
	// Write replaces the secret atomically.
	Write(name string, data []byte) error
	// Remove deletes the secret. Removing a secret that does not exist is not
	// an error.
	Remove(name string) error
}

// New returns the Store for the storage directory dir.
func New(dir string, cfg config.Secrets) (Store, error) {
	files := fileStore{dir: dir}
	if cfg.KeySource == config.SecretsKeySourceNone {
		return files, nil
	}
	source, err := newKeySource(dir, cfg)
	if err != nil {
		return nil, err
	}
	key, err := source.key()
	if err != nil {
		return nil, fmt.Errorf("load the secrets key from %s: %w", cfg.KeySource, err)
	}
	return newEncryptedStore(files, key)
}

// encryptedPrefix marks a file written by encryptedStore.
var encryptedPrefix = []byte("otel-opampsupervisor-secret-v1\n")

// fileStore keeps secrets unencrypted, readable only by the Supervisor's user.
type fileStore struct {
	dir string
}

func (f fileStore) Read(name string) ([]byte, error) {
	data, err := f.readRaw(name)
	if err != nil {
		return nil, err
	}
	if bytes.HasPrefix(data, encryptedPrefix) {
		return nil, fmt.Errorf("%s is encrypted; configure storage::secrets::key_source to read it", name)
	}
	return data, nil
}

func (f fileStore) readRaw(name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(f.dir, name))
}

func (f fileStore) Write(name string, data []byte) error {
	return writeFileAtomic(filepath.Join(f.dir, name), data)
}

func (f fileStore) Remove(name string) error {
	err := os.Remove(filepath.Join(f.dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// writeFileAtomic writes data to a temporary file in the same directory and
// renames it over path, so a crash never leaves a partially written secret.
func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	if writeErr := writeAndClose(tmp, data); writeErr != nil {
		_ = os.Remove(tmp.Name())
		return writeErr
	}
	if renameErr := os.Rename(tmp.Name(), path); renameErr != nil {
		_ = os.Remove(tmp.Name())
		return renameErr
	}
	syncDir(filepath.Dir(path))
	return nil
}

// syncDir makes a rename in dir durable. It is best effort: Windows cannot
// sync a directory.
func syncDir(dir string) {
	if runtime.GOOS == "windows" {
		return
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

func writeAndClose(f *os.File, data []byte) error {
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// encryptedStore encrypts secrets with AES-256-GCM. The secret's name is
// authenticated with it, so one secret's file cannot be swapped for another's.
type encryptedStore struct {
	files fileStore
	aead  cipher.AEAD
}

func newEncryptedStore(files fileStore, key []byte) (*encryptedStore, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &encryptedStore{files: files, aead: aead}, nil
}

func (e *encryptedStore) Read(name string) ([]byte, error) {
	data, err := e.files.readRaw(name)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(data, encryptedPrefix) {
		// Written before encryption was configured: encrypt it in place.
		if writeErr := e.Write(name, data); writeErr != nil {
			return nil, fmt.Errorf("encrypt %s: %w", name, writeErr)
		}
		return data, nil
	}
	sealed := data[len(encryptedPrefix):]
	nonceSize := e.aead.NonceSize()
	if len(sealed) < nonceSize {
		return nil, fmt.Errorf("%s is truncated", name)
	}
	plain, err := e.aead.Open(nil, sealed[:nonceSize], sealed[nonceSize:], []byte(name))
	if err != nil {
		return nil, fmt.Errorf("decrypt %s: %w", name, err)
	}
	return plain, nil
}

func (e *encryptedStore) Write(name string, data []byte) error {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	out := make([]byte, 0, len(encryptedPrefix)+len(nonce)+len(data)+e.aead.Overhead())
	out = append(out, encryptedPrefix...)
	out = append(out, nonce...)
	out = e.aead.Seal(out, nonce, data, []byte(name))
	return e.files.Write(name, out)
}

func (e *encryptedStore) Remove(name string) error {
	return e.files.Remove(name)
}
