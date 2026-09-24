// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"encoding/hex"
	"errors"
	"os"

	"github.com/google/uuid"
	"github.com/open-telemetry/opamp-go/protobufs"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// persistentState represents persistent state for the supervisor
type persistentState struct {
	// InstanceID must be a valid UUID string. If it is not, a new UUIDv7 will be generated automatically.
	InstanceID             uuid.UUID           `yaml:"instance_id"`
	LastRemoteConfigStatus *RemoteConfigStatus `yaml:"last_remote_config_status"`
	// OpAMPConnectionSettings describes server-offered OpAMP connection
	// settings persisted in the storage directory, if any.
	OpAMPConnectionSettings *OpAMPConnectionSettingsState `yaml:"opamp_connection_settings,omitempty"`

	// Path to the config file that the state should be saved to.
	// This is not marshaled.
	configPath string      `yaml:"-"`
	logger     *zap.Logger `yaml:"-"`
}

// RemoteConfigStatus is a custom struct that is used to marshal/unmarshal the remote config status.
// LastRemoteConfigHash is a hex encoded string of the last remote config hash for human readability.
type RemoteConfigStatus struct {
	// Status is the status of the last remote config.
	Status protobufs.RemoteConfigStatuses `yaml:"status"`
	// LastRemoteConfigHash is a hex encoded string of the last remote config hash for human readability.
	LastRemoteConfigHash string `yaml:"last_remote_config_hash"`
	// ErrorMessage is the error message of the last remote config.
	ErrorMessage string `yaml:"error_message"`
}

// OpAMPConnectionSettingsState describes persisted server-offered OpAMP
// connection settings.
type OpAMPConnectionSettingsState struct {
	// BootstrapHash identifies the server settings from the Supervisor's config
	// file that the persisted settings replace. When the config file changes,
	// the persisted settings are discarded.
	BootstrapHash string `yaml:"bootstrap_hash"`
	// LastStatus is the status of the last connection settings offer.
	LastStatus *ConnectionSettingsStatus `yaml:"last_status,omitempty"`
}

// ConnectionSettingsStatus is the persisted form of a connection settings
// status. LastConnectionSettingsHash is hex encoded for human readability.
type ConnectionSettingsStatus struct {
	Status                     protobufs.ConnectionSettingsStatuses `yaml:"status"`
	LastConnectionSettingsHash string                               `yaml:"last_connection_settings_hash"`
	ErrorMessage               string                               `yaml:"error_message"`
}

func (p *persistentState) SetOpAMPConnectionSettingsBootstrapHash(hash string) error {
	if p.OpAMPConnectionSettings == nil {
		p.OpAMPConnectionSettings = &OpAMPConnectionSettingsState{}
	}
	p.OpAMPConnectionSettings.BootstrapHash = hash
	return p.writeState()
}

func (p *persistentState) SetLastConnectionSettingsStatus(status *protobufs.ConnectionSettingsStatus) error {
	p.setLastConnectionSettingsStatus(status)
	return p.writeState()
}

// SetOpAMPConnectionSettingsOffer records, in one write, the config file the
// offered settings replace and the offer's status.
func (p *persistentState) SetOpAMPConnectionSettingsOffer(bootstrapHash string, status *protobufs.ConnectionSettingsStatus) error {
	if p.OpAMPConnectionSettings == nil {
		p.OpAMPConnectionSettings = &OpAMPConnectionSettingsState{}
	}
	// A status without a hash cannot be reported; keep the previous one.
	if len(status.LastConnectionSettingsHash) > 0 {
		p.setLastConnectionSettingsStatus(status)
	}
	p.OpAMPConnectionSettings.BootstrapHash = bootstrapHash
	return p.writeState()
}

func (p *persistentState) setLastConnectionSettingsStatus(status *protobufs.ConnectionSettingsStatus) {
	if p.OpAMPConnectionSettings == nil {
		p.OpAMPConnectionSettings = &OpAMPConnectionSettingsState{}
	}
	p.OpAMPConnectionSettings.LastStatus = &ConnectionSettingsStatus{
		Status:                     status.Status,
		LastConnectionSettingsHash: hex.EncodeToString(status.LastConnectionSettingsHash),
		ErrorMessage:               status.ErrorMessage,
	}
}

func (p *persistentState) GetLastConnectionSettingsStatus() *protobufs.ConnectionSettingsStatus {
	if p.OpAMPConnectionSettings == nil || p.OpAMPConnectionSettings.LastStatus == nil {
		return nil
	}
	last := p.OpAMPConnectionSettings.LastStatus
	hash, err := hex.DecodeString(last.LastConnectionSettingsHash)
	if err != nil || len(hash) == 0 {
		return nil
	}
	return &protobufs.ConnectionSettingsStatus{
		Status:                     last.Status,
		LastConnectionSettingsHash: hash,
		ErrorMessage:               last.ErrorMessage,
	}
}

func (p *persistentState) ClearOpAMPConnectionSettings() error {
	p.OpAMPConnectionSettings = nil
	return p.writeState()
}

func (p *persistentState) SetInstanceID(id uuid.UUID) error {
	p.InstanceID = id
	return p.writeState()
}

func (p *persistentState) SetLastRemoteConfigStatus(status *protobufs.RemoteConfigStatus) error {
	p.LastRemoteConfigStatus = &RemoteConfigStatus{
		Status:               status.Status,
		LastRemoteConfigHash: hex.EncodeToString(status.LastRemoteConfigHash),
		ErrorMessage:         status.ErrorMessage,
	}
	return p.writeState()
}

func (p *persistentState) GetLastRemoteConfigStatus() *protobufs.RemoteConfigStatus {
	if p.LastRemoteConfigStatus == nil {
		return nil
	}
	lastRemoteConfigHash, err := hex.DecodeString(p.LastRemoteConfigStatus.LastRemoteConfigHash)
	if err != nil {
		p.logger.Error("Failed to decode last remote config hash, returning empty status", zap.Error(err))
		return nil
	}
	return &protobufs.RemoteConfigStatus{
		Status:               p.LastRemoteConfigStatus.Status,
		LastRemoteConfigHash: lastRemoteConfigHash,
		ErrorMessage:         p.LastRemoteConfigStatus.ErrorMessage,
	}
}

func (p *persistentState) writeState() error {
	by, err := yaml.Marshal(p)
	if err != nil {
		return err
	}

	return os.WriteFile(p.configPath, by, 0o600)
}

// loadOrCreatePersistentState attempts to load the persistent state from disk. If it doesn't
// exist, a new persistent state file is created.
// instanceID must be a valid UUID string, or an empty string to generate a new UUIDv7 automatically.
func loadOrCreatePersistentState(file, instanceID string, logger *zap.Logger) (*persistentState, error) {
	state, err := loadPersistentState(file, logger)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return createNewPersistentState(file, instanceID, logger)
	case err != nil:
		return nil, err
	default:
		return state, nil
	}
}

func loadPersistentState(file string, logger *zap.Logger) (*persistentState, error) {
	var state *persistentState

	by, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(by, &state); err != nil {
		return nil, err
	}

	state.configPath = file
	state.logger = logger

	return state, nil
}

func createNewPersistentState(file, instanceID string, logger *zap.Logger) (*persistentState, error) {
	id, err := uuid.Parse(instanceID)
	if err != nil {
		if instanceID != "" {
			logger.Warn("Failed to parse instance_id, generating one automatically", zap.Error(err))
		}
		id, err = uuid.NewV7()
		if err != nil {
			return nil, err
		}
	}

	p := &persistentState{
		InstanceID: id,
		configPath: file,
		logger:     logger,
	}

	err = p.writeState()
	return p, err
}
