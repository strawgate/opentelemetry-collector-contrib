// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-telemetry/opamp-go/protobufs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/secretstore"
)

// Server-offered OpAMP connection settings follow the OpAMP specification's
// connection settings offer flow: an offer is saved as a candidate, the
// Supervisor reconnects with it, and it becomes the valid settings only once
// the server accepts the connection. Otherwise the candidate is dropped and
// the previous settings are restored. A candidate found at startup, left by a
// Supervisor that stopped mid-offer, is verified again.
// https://github.com/open-telemetry/opamp-spec/blob/main/specification.md#opamp-connection-setting-offer-flow

const (
	offeredConnectionSettingsFile          = "opamp_connection_settings.dat"
	offeredConnectionSettingsCandidateFile = "opamp_connection_settings.candidate.dat"
)

// connectionSettingsVerifyTimeout bounds how long the server may take to
// accept a connection made with offered settings. opamp-go raises no callback
// when a plain-HTTP request is rejected with 401 or 403, so this timeout is
// what reverts such an offer.
var connectionSettingsVerifyTimeout = 60 * time.Second

// offeredConnectionSettings holds the state for server-offered OpAMP connection
// settings. It is nil unless the feature gate and
// accepts_opamp_connection_settings are enabled.
type offeredConnectionSettings struct {
	// mu serializes offers, including a candidate verified at startup.
	mu      sync.Mutex
	secrets secretstore.Store
	// bootstrapHash identifies the server settings from the config file.
	bootstrapHash string
	// valid is the last offered settings the server accepted, or nil.
	valid *protobufs.OpAMPConnectionSettings
	// verification is the pending check of a connection made with new settings.
	verification atomic.Pointer[connectionVerification]
	// offerHash is the hash of the latest ConnectionSettingsOffers received.
	offerHash atomic.Pointer[[]byte]
}

// errVerificationAborted means the Supervisor shut down before the server
// accepted or refused a connection. The candidate is kept and verified again
// at the next start.
var errVerificationAborted = errors.New("the Supervisor shut down while verifying offered OpAMP connection settings")

// connectionVerification records whether the server accepted a connection.
// Failed attempts are retried by the OpAMP client, so they only end the
// verification once the timeout passes; the last one is reported.
type connectionVerification struct {
	accepted chan struct{}
	once     sync.Once

	mu      sync.Mutex
	lastErr error
}

func newConnectionVerification() *connectionVerification {
	return &connectionVerification{accepted: make(chan struct{})}
}

func (v *connectionVerification) accept() {
	v.once.Do(func() { close(v.accepted) })
}

func (v *connectionVerification) recordFailure(err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.lastErr = err
}

func (v *connectionVerification) lastFailure() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.lastErr
}

// pendingConnectionSettings is a candidate found at startup, still to be
// verified against the server.
type pendingConnectionSettings struct {
	candidate                *protobufs.OpAMPConnectionSettings
	hash                     []byte
	fallback                 config.OpAMPServer
	fallbackHeartbeatSeconds uint64
	verification             *connectionVerification
}

func validateOfferedConnectionSettingsConfig(cfg config.Supervisor) error {
	gate := metadata.CmdOpampsupervisorPersistOpAMPConnectionSettingsFeatureGate
	usesGate := cfg.Capabilities.ReportsConnectionSettingsStatus || cfg.Storage.Secrets.KeySource != config.SecretsKeySourceNone
	if usesGate && !gate.IsEnabled() {
		return fmt.Errorf(
			"capabilities::reports_connection_settings_status and storage::secrets require the %q feature gate; enable it with --feature-gates=%s",
			gate.ID(), gate.ID(),
		)
	}
	if cfg.Capabilities.ReportsConnectionSettingsStatus && !cfg.Capabilities.AcceptsOpAMPConnectionSettings {
		return errors.New("capabilities::reports_connection_settings_status requires capabilities::accepts_opamp_connection_settings")
	}
	return nil
}

// newServerConfigFromOpAMPSettings builds the server config for offered
// settings. The configured auth extension is kept; the server only offers the
// endpoint, headers and TLS settings.
func newServerConfigFromOpAMPSettings(auth component.ID, settings *protobufs.OpAMPConnectionSettings) (config.OpAMPServer, error) {
	server := config.OpAMPServer{
		Auth: auth,
	}

	if settings.DestinationEndpoint != "" {
		server.Endpoint = settings.DestinationEndpoint
	}
	if settings.Headers != nil {
		server.Headers = make(http.Header)
		for _, header := range settings.Headers.Headers {
			server.Headers.Add(header.Key, header.Value)
		}
	}
	if settings.Certificate != nil {
		if len(settings.Certificate.CaCert) != 0 {
			server.TLS.CAPem = configopaque.String(settings.Certificate.CaCert)
		}
		if len(settings.Certificate.Cert) != 0 {
			server.TLS.CertPem = configopaque.String(settings.Certificate.Cert)
		}
		if len(settings.Certificate.PrivateKey) != 0 {
			server.TLS.KeyPem = configopaque.String(settings.Certificate.PrivateKey)
		}
	} else {
		server.TLS = configtls.NewDefaultClientConfig()
		server.TLS.InsecureSkipVerify = true
	}

	if err := server.Validate(); err != nil {
		return config.OpAMPServer{}, err
	}
	return server, nil
}

// opampServerHash fingerprints server settings, including secret values, so a
// change to the config file can be detected. It is not stored anywhere the
// secrets themselves are not.
func opampServerHash(server config.OpAMPServer) (string, error) {
	tls := server.TLS
	b, err := json.Marshal(struct {
		Endpoint                 string
		Headers                  http.Header
		CAFile, CertFile         string
		KeyFile                  string
		CAPem, CertPem, KeyPem   string
		Insecure, SkipVerify     bool
		IncludeSystemCACertsPool bool
		ServerName               string
		MinVersion, MaxVersion   string
		Auth                     string
	}{
		Endpoint:                 server.Endpoint,
		Headers:                  server.Headers,
		CAFile:                   tls.CAFile,
		CertFile:                 tls.CertFile,
		KeyFile:                  tls.KeyFile,
		CAPem:                    string(tls.CAPem),
		CertPem:                  string(tls.CertPem),
		KeyPem:                   string(tls.KeyPem),
		Insecure:                 tls.Insecure,
		SkipVerify:               tls.InsecureSkipVerify,
		IncludeSystemCACertsPool: tls.IncludeSystemCACertsPool,
		ServerName:               tls.ServerName,
		MinVersion:               tls.MinVersion,
		MaxVersion:               tls.MaxVersion,
		Auth:                     server.Auth.String(),
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func (o *offeredConnectionSettings) read(name string) (*protobufs.OpAMPConnectionSettings, error) {
	data, err := o.secrets.Read(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	settings := &protobufs.OpAMPConnectionSettings{}
	if err := proto.Unmarshal(data, settings); err != nil {
		return nil, fmt.Errorf("parse %s: %w", name, err)
	}
	return settings, nil
}

func (o *offeredConnectionSettings) write(name string, settings *protobufs.OpAMPConnectionSettings) error {
	data, err := proto.Marshal(settings)
	if err != nil {
		return err
	}
	return o.secrets.Write(name, data)
}

func (o *offeredConnectionSettings) beginVerification() *connectionVerification {
	v := newConnectionVerification()
	o.verification.Store(v)
	return v
}

func (o *offeredConnectionSettings) lastOfferHash() []byte {
	if hash := o.offerHash.Load(); hash != nil {
		return *hash
	}
	return nil
}

// loadOfferedConnectionSettings restores server-offered OpAMP connection
// settings persisted by an earlier run, before the first connection to the
// server. It returns a candidate that still has to be verified, or nil.
func (s *Supervisor) loadOfferedConnectionSettings() (*pendingConnectionSettings, error) {
	if !metadata.CmdOpampsupervisorPersistOpAMPConnectionSettingsFeatureGate.IsEnabled() ||
		!s.config.Capabilities.AcceptsOpAMPConnectionSettings {
		return nil, nil
	}
	logger := s.telemetrySettings.Logger

	store, err := secretstore.New(s.config.Storage.Directory, s.config.Storage.Secrets)
	if err != nil {
		return nil, fmt.Errorf("cannot open the secret store: %w", err)
	}
	bootstrapHash, err := opampServerHash(s.config.Server)
	if err != nil {
		return nil, err
	}
	offered := &offeredConnectionSettings{secrets: store, bootstrapHash: bootstrapHash}
	s.offeredConnSettings = offered

	if state := s.persistentState.OpAMPConnectionSettings; state != nil && state.BootstrapHash != "" && state.BootstrapHash != bootstrapHash {
		logger.Info("The server settings in the config file changed; discarding OpAMP connection settings offered by the server")
		for _, name := range []string{offeredConnectionSettingsFile, offeredConnectionSettingsCandidateFile} {
			if removeErr := store.Remove(name); removeErr != nil {
				logger.Error("Cannot remove persisted OpAMP connection settings", zap.String("file", name), zap.Error(removeErr))
			}
		}
		if clearErr := s.persistentState.ClearOpAMPConnectionSettings(); clearErr != nil {
			logger.Error("Cannot update the persistent state", zap.Error(clearErr))
		}
	}

	valid, err := offered.read(offeredConnectionSettingsFile)
	if err != nil {
		logger.Error("Cannot read persisted OpAMP connection settings; using the config file", zap.Error(err))
	}
	if valid != nil {
		server, parseErr := newServerConfigFromOpAMPSettings(s.config.Server.Auth, valid)
		if parseErr != nil {
			logger.Error("Persisted OpAMP connection settings are invalid; using the config file", zap.Error(parseErr))
		} else {
			s.config.Server = server
			s.applyOfferedHeartbeatInterval(valid)
			offered.valid = valid
			logger.Info("Using OpAMP connection settings offered by the server", zap.String("endpoint", server.Endpoint))
		}
	}

	candidate, err := offered.read(offeredConnectionSettingsCandidateFile)
	if err != nil {
		logger.Error("Cannot read the OpAMP connection settings being applied when the Supervisor stopped", zap.Error(err))
	}
	if candidate == nil {
		return nil, nil
	}
	server, err := newServerConfigFromOpAMPSettings(s.config.Server.Auth, candidate)
	if err != nil {
		logger.Error("Discarding invalid OpAMP connection settings", zap.Error(err))
		_ = store.Remove(offeredConnectionSettingsCandidateFile)
		return nil, nil
	}

	pending := &pendingConnectionSettings{
		candidate:                candidate,
		fallback:                 s.config.Server,
		fallbackHeartbeatSeconds: s.heartbeatIntervalSeconds,
	}
	if status := s.persistentState.GetLastConnectionSettingsStatus(); status != nil {
		pending.hash = status.LastConnectionSettingsHash
	}
	s.config.Server = server
	s.applyOfferedHeartbeatInterval(candidate)
	pending.verification = offered.beginVerification()
	logger.Info("Verifying OpAMP connection settings that were being applied when the Supervisor stopped",
		zap.String("endpoint", server.Endpoint))
	return pending, nil
}

// finishPendingConnectionSettings verifies a candidate found at startup. The
// caller holds offeredConnSettings.mu, which this releases.
func (s *Supervisor) finishPendingConnectionSettings(pending *pendingConnectionSettings) {
	defer s.offeredConnSettings.mu.Unlock()
	err := s.awaitConnectionVerification(pending.verification)
	if errors.Is(err, errVerificationAborted) {
		s.telemetrySettings.Logger.Info("Stopping before offered OpAMP connection settings were verified; they will be verified at the next start")
		return
	}
	s.concludeConnectionSettingsOffer(pending.candidate, pending.hash, err, pending.fallback, pending.fallbackHeartbeatSeconds)
}

// saveConnectionSettingsCandidate persists an offer before the Supervisor
// connects with it, so a Supervisor that stops mid-offer verifies it again at
// the next start. The status and the config file it replaces are recorded
// first, so the candidate is never found without them.
func (s *Supervisor) saveConnectionSettingsCandidate(settings *protobufs.OpAMPConnectionSettings, hash []byte) error {
	err := s.persistentState.SetOpAMPConnectionSettingsOffer(s.offeredConnSettings.bootstrapHash, &protobufs.ConnectionSettingsStatus{
		LastConnectionSettingsHash: hash,
		Status:                     protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLYING,
	})
	if err != nil {
		return err
	}
	return s.offeredConnSettings.write(offeredConnectionSettingsCandidateFile, settings)
}

// onOfferedConnectionSettings applies connection settings offered by the
// server: it reconnects with them and keeps them only if the server accepts
// the connection.
func (s *Supervisor) onOfferedConnectionSettings(settings *protobufs.OpAMPConnectionSettings, hash []byte) {
	offered := s.offeredConnSettings
	offered.mu.Lock()
	defer offered.mu.Unlock()
	logger := s.telemetrySettings.Logger

	if settings == nil {
		logger.Debug("Received ConnectionSettings request with nil settings")
		return
	}

	server, err := newServerConfigFromOpAMPSettings(s.config.Server.Auth, settings)
	if err != nil {
		logger.Error("New OpAMP settings resulted in invalid configuration", zap.Error(err))
		s.reportConnectionSettingsStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_FAILED, err.Error())
		return
	}

	// The server re-offers the settings in use, e.g. after the Supervisor
	// restarted.
	if offered.valid != nil && proto.Equal(offered.valid, settings) {
		s.reportConnectionSettingsStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLIED, "")
		return
	}

	// The APPLYING status saved with it is sent in the first message on the
	// new connection.
	if saveErr := s.saveConnectionSettingsCandidate(settings, hash); saveErr != nil {
		// Settings that cannot be persisted would be lost on restart.
		logger.Error("Cannot persist the offered OpAMP connection settings; rejecting them", zap.Error(saveErr))
		_ = offered.secrets.Remove(offeredConnectionSettingsCandidateFile)
		s.reportConnectionSettingsStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_FAILED,
			"cannot persist the offered settings: "+saveErr.Error())
		return
	}

	previous := s.config.Server
	previousHeartbeatSeconds := s.heartbeatIntervalSeconds

	if stopErr := s.stopOpAMPClient(); stopErr != nil {
		logger.Error("Cannot stop the OpAMP client", zap.Error(stopErr))
		_ = offered.secrets.Remove(offeredConnectionSettingsCandidateFile)
		s.reportConnectionSettingsStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_FAILED, stopErr.Error())
		return
	}

	s.config.Server = server
	s.applyOfferedHeartbeatInterval(settings)
	verification := offered.beginVerification()
	err = s.startOpAMPClient()
	if err == nil {
		err = s.awaitConnectionVerification(verification)
	}
	if errors.Is(err, errVerificationAborted) {
		logger.Info("Stopping before offered OpAMP connection settings were verified; they will be verified at the next start")
		return
	}
	s.concludeConnectionSettingsOffer(settings, hash, err, previous, previousHeartbeatSeconds)
}

// concludeConnectionSettingsOffer keeps settings the server accepted, or
// restores the previous settings.
func (s *Supervisor) concludeConnectionSettingsOffer(
	settings *protobufs.OpAMPConnectionSettings,
	hash []byte,
	verifyErr error,
	previous config.OpAMPServer,
	previousHeartbeatSeconds uint64,
) {
	offered := s.offeredConnSettings
	logger := s.telemetrySettings.Logger

	if verifyErr == nil {
		if err := offered.write(offeredConnectionSettingsFile, settings); err != nil {
			logger.Error("Cannot persist the accepted OpAMP connection settings; they will be lost on restart", zap.Error(err))
		}
		if err := offered.secrets.Remove(offeredConnectionSettingsCandidateFile); err != nil {
			logger.Error("Cannot remove the candidate OpAMP connection settings", zap.Error(err))
		}
		offered.valid = settings
		if err := s.persistentState.SetOpAMPConnectionSettingsBootstrapHash(offered.bootstrapHash); err != nil {
			logger.Error("Cannot update the persistent state", zap.Error(err))
		}
		s.reportConnectionSettingsStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLIED, "")
		logger.Info("Applied OpAMP connection settings offered by the server", zap.String("endpoint", s.config.Server.Endpoint))
		return
	}

	logger.Error("The OpAMP server did not accept a connection with the offered settings; restoring the previous settings", zap.Error(verifyErr))
	if err := offered.secrets.Remove(offeredConnectionSettingsCandidateFile); err != nil {
		logger.Error("Cannot remove the candidate OpAMP connection settings", zap.Error(err))
	}
	// Sent in the first message on the restored connection.
	s.saveConnectionSettingsStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_FAILED, verifyErr.Error())

	if err := s.stopOpAMPClient(); err != nil {
		logger.Error("Cannot stop the OpAMP client", zap.Error(err))
	}
	s.config.Server = previous
	s.heartbeatIntervalSeconds = previousHeartbeatSeconds
	if err := s.startOpAMPClient(); err != nil {
		logger.Error("Cannot reconnect to the OpAMP server after restoring old settings", zap.Error(err))
	}
}

// awaitConnectionVerification waits until the server accepts a connection or
// the verify timeout passes. It returns errVerificationAborted if the
// Supervisor shuts down first.
func (s *Supervisor) awaitConnectionVerification(v *connectionVerification) error {
	defer s.offeredConnSettings.verification.CompareAndSwap(v, nil)
	timer := time.NewTimer(connectionSettingsVerifyTimeout)
	defer timer.Stop()
	select {
	case <-v.accepted:
	case <-timer.C:
		if s.runCtx.Err() != nil {
			return errVerificationAborted
		}
		err := fmt.Errorf("the server did not accept a connection within %s", connectionSettingsVerifyTimeout)
		if last := v.lastFailure(); last != nil {
			err = fmt.Errorf("%w; last error: %w", err, last)
		}
		return err
	case <-s.runCtx.Done():
		return errVerificationAborted
	}
	// Checked after the select, which picks at random when both are ready.
	if s.runCtx.Err() != nil {
		return errVerificationAborted
	}
	return nil
}

// observeOpAMPConnected records that the server accepted a connection. For
// WebSocket that is a completed upgrade; for plain HTTP a 200 response.
func (s *Supervisor) observeOpAMPConnected() {
	if s.offeredConnSettings == nil {
		return
	}
	if v := s.offeredConnSettings.verification.Load(); v != nil {
		v.accept()
	}
}

// observeOpAMPConnectFailed records a failed connection attempt, e.g. a
// rejected WebSocket upgrade, a TLS error or an unreachable server. The OpAMP
// client retries, so this does not end the verification.
func (s *Supervisor) observeOpAMPConnectFailed(err error) {
	if s.offeredConnSettings == nil || err == nil {
		return
	}
	if v := s.offeredConnSettings.verification.Load(); v != nil {
		v.recordFailure(err)
	}
}

// observeConnectionSettingsOffer records the offer hash of each message, which
// opamp-go passes to OnMessage before OnOpampConnectionSettings, so an offer
// without a hash is never reported under an earlier offer's hash.
func (s *Supervisor) observeConnectionSettingsOffer(hash []byte) {
	if s.offeredConnSettings != nil {
		hash = append([]byte(nil), hash...)
		s.offeredConnSettings.offerHash.Store(&hash)
	}
}

func (s *Supervisor) applyOfferedHeartbeatInterval(settings *protobufs.OpAMPConnectionSettings) {
	// Ignore non-positive intervals from the server; see onOpampConnectionSettings.
	if s.config.Capabilities.ReportsHeartbeat && settings.HeartbeatIntervalSeconds > 0 {
		s.heartbeatIntervalSeconds = settings.HeartbeatIntervalSeconds
	}
}

// lastConnectionSettingsStatus is reported in the first message of each new
// connection.
func (s *Supervisor) lastConnectionSettingsStatus() *protobufs.ConnectionSettingsStatus {
	if s.offeredConnSettings == nil || !s.config.Capabilities.ReportsConnectionSettingsStatus || s.persistentState == nil {
		return nil
	}
	return s.persistentState.GetLastConnectionSettingsStatus()
}

func (s *Supervisor) saveConnectionSettingsStatus(hash []byte, status protobufs.ConnectionSettingsStatuses, errorMessage string) {
	if len(hash) == 0 {
		return
	}
	err := s.persistentState.SetLastConnectionSettingsStatus(&protobufs.ConnectionSettingsStatus{
		LastConnectionSettingsHash: hash,
		Status:                     status,
		ErrorMessage:               errorMessage,
	})
	if err != nil {
		s.telemetrySettings.Logger.Error("Cannot save the connection settings status", zap.Error(err))
	}
}

// reportConnectionSettingsStatus saves the status and reports it on the
// current connection.
func (s *Supervisor) reportConnectionSettingsStatus(hash []byte, status protobufs.ConnectionSettingsStatuses, errorMessage string) {
	s.saveConnectionSettingsStatus(hash, status, errorMessage)
	if !s.config.Capabilities.ReportsConnectionSettingsStatus || len(hash) == 0 {
		return
	}
	err := s.opampClient.SetConnectionSettingsStatus(&protobufs.ConnectionSettingsStatus{
		LastConnectionSettingsHash: hash,
		Status:                     status,
		ErrorMessage:               errorMessage,
	})
	if err != nil {
		s.telemetrySettings.Logger.Error("Cannot report the connection settings status", zap.Error(err))
	}
}
