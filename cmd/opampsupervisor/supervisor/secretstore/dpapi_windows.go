// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package secretstore // import "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/secretstore"

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// dpapiKeySource keeps the key in a file protected with the Windows Data
// Protection API for the account the Supervisor runs as, so only that account
// can read it. The key is created on first use.
type dpapiKeySource struct {
	path string
}

func newDPAPIKeySource(path string) (keySource, error) {
	return dpapiKeySource{path: path}, nil
}

func (d dpapiKeySource) key() ([]byte, error) {
	protected, err := os.ReadFile(d.path)
	if err == nil {
		key, unprotectErr := dpapiUnprotect(protected)
		if unprotectErr != nil {
			return nil, fmt.Errorf("unprotect %s: %w", d.path, unprotectErr)
		}
		return decodeKey(key)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	key, err := newRandomKey()
	if err != nil {
		return nil, err
	}
	protected, err = dpapiProtect(key)
	if err != nil {
		return nil, fmt.Errorf("protect the key: %w", err)
	}
	if err := writeFileAtomic(d.path, protected); err != nil {
		return nil, err
	}
	return key, nil
}

func dpapiProtect(data []byte) ([]byte, error) {
	in := windows.DataBlob{Size: uint32(len(data)), Data: &data[0]}
	var out windows.DataBlob
	if err := windows.CryptProtectData(&in, nil, nil, 0, nil, windows.CRYPTPROTECT_UI_FORBIDDEN, &out); err != nil {
		return nil, err
	}
	return takeBlob(&out), nil
}

func dpapiUnprotect(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("empty key file")
	}
	in := windows.DataBlob{Size: uint32(len(data)), Data: &data[0]}
	var out windows.DataBlob
	if err := windows.CryptUnprotectData(&in, nil, nil, 0, nil, windows.CRYPTPROTECT_UI_FORBIDDEN, &out); err != nil {
		return nil, err
	}
	return takeBlob(&out), nil
}

// takeBlob copies a DPAPI output buffer into Go memory and frees it.
func takeBlob(blob *windows.DataBlob) []byte {
	defer func() {
		_, _ = windows.LocalFree(windows.Handle(unsafe.Pointer(blob.Data)))
	}()
	return append([]byte(nil), unsafe.Slice(blob.Data, blob.Size)...)
}
