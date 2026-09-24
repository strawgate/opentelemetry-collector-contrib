// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server"
	serverTypes "github.com/open-telemetry/opamp-go/server/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/telemetry"
)

const (
	bootstrapAuth = "Bearer bootstrap"
	agentAuth     = "Bearer agent-1"
)

func enablePersistConnectionSettingsGate(t *testing.T) {
	t.Helper()
	gate := metadata.CmdOpampsupervisorPersistOpAMPConnectionSettingsFeatureGate
	require.NoError(t, featuregate.GlobalRegistry().Set(gate.ID(), true))
	t.Cleanup(func() {
		require.NoError(t, featuregate.GlobalRegistry().Set(gate.ID(), false))
	})
}

func setVerifyTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	previous := connectionSettingsVerifyTimeout
	connectionSettingsVerifyTimeout = d
	t.Cleanup(func() { connectionSettingsVerifyTimeout = previous })
}

type nopOpAMPLogger struct{}

func (nopOpAMPLogger) Debugf(context.Context, string, ...any) {}
func (nopOpAMPLogger) Errorf(context.Context, string, ...any) {}

// offerTestServer is an OpAMP server that accepts only the Authorization
// values it allows, answering others with 401 over both transports.
type offerTestServer struct {
	httpURL string

	mu       sync.Mutex
	allowed  map[string]bool
	accepted []string
	statuses []*protobufs.ConnectionSettingsStatus
	conn     serverTypes.Connection
	// onAccept, if set, runs when a request or connection is accepted.
	onAccept func(auth string)
}

func newOfferTestServer(t *testing.T, allowed ...string) *offerTestServer {
	t.Helper()
	ts := &offerTestServer{allowed: map[string]bool{}}
	for _, a := range allowed {
		ts.allowed[a] = true
	}

	srv := server.New(nopOpAMPLogger{})
	handler, connContext, err := srv.Attach(server.Settings{
		Callbacks: serverTypes.Callbacks{
			OnConnecting: func(req *http.Request) serverTypes.ConnectionResponse {
				auth := req.Header.Get("Authorization")
				ts.mu.Lock()
				defer ts.mu.Unlock()
				if !ts.allowed[auth] {
					return serverTypes.ConnectionResponse{Accept: false, HTTPStatusCode: http.StatusUnauthorized}
				}
				ts.accepted = append(ts.accepted, auth)
				if ts.onAccept != nil {
					ts.onAccept(auth)
				}
				return serverTypes.ConnectionResponse{
					Accept: true,
					ConnectionCallbacks: serverTypes.ConnectionCallbacks{
						OnConnected: func(_ context.Context, conn serverTypes.Connection) {
							ts.mu.Lock()
							ts.conn = conn
							ts.mu.Unlock()
						},
						OnMessage: func(_ context.Context, _ serverTypes.Connection, msg *protobufs.AgentToServer) *protobufs.ServerToAgent {
							if msg.ConnectionSettingsStatus != nil {
								ts.mu.Lock()
								ts.statuses = append(ts.statuses, msg.ConnectionSettingsStatus)
								ts.mu.Unlock()
							}
							return &protobufs.ServerToAgent{InstanceUid: msg.InstanceUid}
						},
					},
				}
			},
		},
	})
	require.NoError(t, err)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/opamp", handler)
	httpSrv := httptest.NewUnstartedServer(mux)
	httpSrv.Config.ConnContext = connContext
	httpSrv.Start()
	t.Cleanup(func() {
		_ = srv.Stop(t.Context())
		httpSrv.Close()
	})
	ts.httpURL = httpSrv.URL + "/v1/opamp"
	return ts
}

func (ts *offerTestServer) endpoint(scheme string) string {
	if scheme == "ws" {
		return "ws" + strings.TrimPrefix(ts.httpURL, "http")
	}
	return ts.httpURL
}

func (ts *offerTestServer) acceptedAuth() []string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return append([]string(nil), ts.accepted...)
}

func (ts *offerTestServer) hasStatus(hash []byte, status protobufs.ConnectionSettingsStatuses) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	for _, s := range ts.statuses {
		if s.Status == status && bytes.Equal(s.LastConnectionSettingsHash, hash) {
			return true
		}
	}
	return false
}

func (ts *offerTestServer) send(t *testing.T, msg *protobufs.ServerToAgent) {
	t.Helper()
	ts.mu.Lock()
	conn := ts.conn
	ts.mu.Unlock()
	require.NotNil(t, conn)
	require.NoError(t, conn.Send(t.Context(), msg))
}

// newOfferTestSupervisor builds a Supervisor that accepts OpAMP connection
// settings, with the bootstrap bearer token in its config and its state in dir.
func newOfferTestSupervisor(t *testing.T, endpoint, dir string, secrets config.Secrets) *Supervisor {
	t.Helper()
	if connectionSettingsVerifyTimeout > 10*time.Second {
		// A test whose verification never finishes fails fast.
		setVerifyTimeout(t, 10*time.Second)
	}
	ctx, cancel := context.WithCancel(t.Context())

	mp := metric.NewMeterProvider()
	t.Cleanup(func() { _ = mp.Shutdown(t.Context()) })
	metrics, err := telemetry.NewMetrics(mp)
	require.NoError(t, err)

	set := componenttest.NewNopTelemetrySettings()
	set.Logger = zaptest.NewLogger(t)

	state, err := loadOrCreatePersistentState(filepath.Join(dir, persistentStateFileName), "", set.Logger)
	require.NoError(t, err)

	agentDesc := &atomic.Value{}
	agentDesc.Store(&protobufs.AgentDescription{
		IdentifyingAttributes: []*protobufs.KeyValue{{
			Key:   "service.name",
			Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: "test-collector"}},
		}},
	})

	s := &Supervisor{
		runCtx:            ctx,
		runCtxCancel:      cancel,
		telemetrySettings: telemetrySettings{TelemetrySettings: set},
		config: config.Supervisor{
			Server: config.OpAMPServer{
				Endpoint: endpoint,
				Headers:  http.Header{"Authorization": []string{bootstrapAuth}},
			},
			Capabilities: config.Capabilities{
				AcceptsOpAMPConnectionSettings:  true,
				ReportsConnectionSettingsStatus: true,
			},
			Storage: config.Storage{Directory: dir, Secrets: secrets},
		},
		heartbeatIntervalSeconds:       30,
		persistentState:                state,
		agentDescription:               agentDesc,
		availableComponents:            &atomic.Value{},
		effectiveConfig:                &atomic.Value{},
		cfgState:                       &atomic.Value{},
		agentConfigOwnTelemetrySection: &atomic.Value{},
		agentConn:                      &atomic.Value{},
		metrics:                        metrics,
	}
	t.Cleanup(func() {
		cancel()
		if s.opampClient != nil {
			stopCtx, stopCancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer stopCancel()
			_ = s.opampClient.Stop(stopCtx)
		}
	})
	return s
}

func offer(endpoint, auth string) *protobufs.OpAMPConnectionSettings {
	return &protobufs.OpAMPConnectionSettings{
		DestinationEndpoint: endpoint,
		Headers: &protobufs.Headers{Headers: []*protobufs.Header{
			{Key: "Authorization", Value: auth},
		}},
	}
}

func startOfferTestSupervisor(t *testing.T, s *Supervisor) *pendingConnectionSettings {
	t.Helper()
	pending, err := s.loadOfferedConnectionSettings()
	require.NoError(t, err)
	require.NotNil(t, s.offeredConnSettings)
	require.NoError(t, s.startOpAMPClient())
	return pending
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

func TestOfferedConnectionSettingsApplied(t *testing.T) {
	for _, scheme := range []string{"ws", "http"} {
		t.Run(scheme, func(t *testing.T) {
			enablePersistConnectionSettingsGate(t)
			ts := newOfferTestServer(t, bootstrapAuth, agentAuth)
			dir := t.TempDir()
			var candidateSaved atomic.Bool
			ts.onAccept = func(auth string) {
				if auth == agentAuth && fileExists(t, filepath.Join(dir, offeredConnectionSettingsCandidateFile)) {
					candidateSaved.Store(true)
				}
			}
			s := newOfferTestSupervisor(t, ts.endpoint(scheme), dir, config.Secrets{})
			startOfferTestSupervisor(t, s)

			hash := []byte("offer-1")
			s.onOfferedConnectionSettings(offer(ts.endpoint(scheme), agentAuth), hash)

			assert.Equal(t, agentAuth, s.config.Server.Headers.Get("Authorization"))
			assert.Contains(t, ts.acceptedAuth(), agentAuth, "the Supervisor reconnected with the offered token")
			assert.True(t, candidateSaved.Load(), "the offer is saved as a candidate before connecting with it")
			assert.True(t, fileExists(t, filepath.Join(dir, offeredConnectionSettingsFile)))
			assert.False(t, fileExists(t, filepath.Join(dir, offeredConnectionSettingsCandidateFile)))
			assert.NotEmpty(t, s.persistentState.OpAMPConnectionSettings.BootstrapHash)
			require.Eventually(t, func() bool {
				return ts.hasStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLIED)
			}, 10*time.Second, 50*time.Millisecond, "APPLIED is reported to the server")
			assert.True(t, ts.hasStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLYING),
				"APPLYING is reported on the connection being verified")

			// A restarted Supervisor connects with the offered token.
			restarted := newOfferTestSupervisor(t, ts.endpoint(scheme), dir, config.Secrets{})
			pending, err := restarted.loadOfferedConnectionSettings()
			require.NoError(t, err)
			assert.Nil(t, pending)
			assert.Equal(t, agentAuth, restarted.config.Server.Headers.Get("Authorization"))
		})
	}
}

func TestOfferedConnectionSettingsRejected(t *testing.T) {
	for _, scheme := range []string{"ws", "http"} {
		t.Run(scheme, func(t *testing.T) {
			enablePersistConnectionSettingsGate(t)
			// Failed attempts are retried until the timeout.
			setVerifyTimeout(t, 3*time.Second)
			ts := newOfferTestServer(t, bootstrapAuth)
			dir := t.TempDir()
			s := newOfferTestSupervisor(t, ts.endpoint(scheme), dir, config.Secrets{})
			startOfferTestSupervisor(t, s)

			hash := []byte("offer-bad")
			s.onOfferedConnectionSettings(offer(ts.endpoint(scheme), "Bearer revoked"), hash)

			assert.Equal(t, bootstrapAuth, s.config.Server.Headers.Get("Authorization"), "the previous settings are restored")
			assert.False(t, fileExists(t, filepath.Join(dir, offeredConnectionSettingsFile)))
			assert.False(t, fileExists(t, filepath.Join(dir, offeredConnectionSettingsCandidateFile)))
			status := s.persistentState.GetLastConnectionSettingsStatus()
			require.NotNil(t, status)
			assert.Equal(t, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_FAILED, status.Status)
			assert.Contains(t, status.ErrorMessage, "did not accept a connection")
			if scheme == "ws" {
				// A rejected upgrade raises OnConnectFailed; plain-HTTP 401 raises nothing.
				assert.Contains(t, status.ErrorMessage, "last error: websocket: bad handshake")
			}
			require.Eventually(t, func() bool {
				return ts.hasStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_FAILED)
			}, 10*time.Second, 50*time.Millisecond, "FAILED is reported on the restored connection")
		})
	}
}

func TestOfferedConnectionSettingsTransientFailure(t *testing.T) {
	// A server that briefly refuses the new settings, e.g. mid-deploy, is retried.
	enablePersistConnectionSettingsGate(t)
	ts := newOfferTestServer(t, bootstrapAuth)
	dir := t.TempDir()
	s := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, config.Secrets{})
	startOfferTestSupervisor(t, s)

	time.AfterFunc(time.Second, func() {
		ts.mu.Lock()
		ts.allowed[agentAuth] = true
		ts.mu.Unlock()
	})
	hash := []byte("offer-1")
	s.onOfferedConnectionSettings(offer(ts.endpoint("ws"), agentAuth), hash)

	assert.Equal(t, agentAuth, s.config.Server.Headers.Get("Authorization"))
	assert.True(t, fileExists(t, filepath.Join(dir, offeredConnectionSettingsFile)))
	require.Eventually(t, func() bool {
		return ts.hasStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLIED)
	}, 10*time.Second, 50*time.Millisecond)
}

func TestOfferedConnectionSettingsShutdownKeepsCandidate(t *testing.T) {
	enablePersistConnectionSettingsGate(t)
	ts := newOfferTestServer(t, bootstrapAuth)
	dir := t.TempDir()
	s := newOfferTestSupervisor(t, ts.endpoint("http"), dir, config.Secrets{})
	startOfferTestSupervisor(t, s)

	done := make(chan struct{})
	hash := []byte("offer-1")
	go func() {
		defer close(done)
		s.onOfferedConnectionSettings(offer(ts.endpoint("http"), agentAuth), hash)
	}()
	require.Eventually(t, func() bool { return s.offeredConnSettings.verification.Load() != nil }, 10*time.Second, 10*time.Millisecond)
	s.runCtxCancel()
	<-done

	assert.True(t, fileExists(t, filepath.Join(dir, offeredConnectionSettingsCandidateFile)), "the candidate is kept")
	status := s.persistentState.GetLastConnectionSettingsStatus()
	require.NotNil(t, status)
	assert.Equal(t, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLYING, status.Status, "shutting down is not a failure")

	restarted := newOfferTestSupervisor(t, ts.endpoint("http"), dir, config.Secrets{})
	pending, err := restarted.loadOfferedConnectionSettings()
	require.NoError(t, err)
	require.NotNil(t, pending, "the next start verifies the candidate")
	assert.Equal(t, hash, pending.hash)
}

func TestLoadOfferedConnectionSettingsWithoutBootstrapHash(t *testing.T) {
	// State recorded without the config file's hash is not taken as a config change.
	enablePersistConnectionSettingsGate(t)
	ts := newOfferTestServer(t, bootstrapAuth, agentAuth)
	dir := t.TempDir()
	s := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, config.Secrets{})
	_, err := s.loadOfferedConnectionSettings()
	require.NoError(t, err)
	require.NoError(t, s.offeredConnSettings.write(offeredConnectionSettingsFile, offer(ts.endpoint("ws"), agentAuth)))
	s.saveConnectionSettingsStatus([]byte("offer-1"), protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLIED, "")
	require.Empty(t, s.persistentState.OpAMPConnectionSettings.BootstrapHash)

	restarted := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, config.Secrets{})
	_, err = restarted.loadOfferedConnectionSettings()
	require.NoError(t, err)
	assert.Equal(t, agentAuth, restarted.config.Server.Headers.Get("Authorization"))
}

func TestObserveConnectionSettingsOffer(t *testing.T) {
	s := &Supervisor{offeredConnSettings: &offeredConnectionSettings{}}
	s.observeConnectionSettingsOffer([]byte("offer-1"))
	assert.Equal(t, []byte("offer-1"), s.offeredConnSettings.lastOfferHash())
	// A later message without a hash must not leave the earlier one in place.
	s.observeConnectionSettingsOffer(nil)
	assert.Empty(t, s.offeredConnSettings.lastOfferHash())
}

func TestOfferedConnectionSettingsInvalidOffer(t *testing.T) {
	enablePersistConnectionSettingsGate(t)
	ts := newOfferTestServer(t, bootstrapAuth)
	dir := t.TempDir()
	s := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, config.Secrets{})
	startOfferTestSupervisor(t, s)
	client := s.opampClient

	hash := []byte("offer-invalid")
	s.onOfferedConnectionSettings(offer("", agentAuth), hash)

	assert.Same(t, client, s.opampClient, "an invalid offer does not reconnect")
	assert.Equal(t, bootstrapAuth, s.config.Server.Headers.Get("Authorization"))
	require.Eventually(t, func() bool {
		return ts.hasStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_FAILED)
	}, 10*time.Second, 50*time.Millisecond)
}

func TestOfferedConnectionSettingsReoffered(t *testing.T) {
	enablePersistConnectionSettingsGate(t)
	ts := newOfferTestServer(t, bootstrapAuth, agentAuth)
	dir := t.TempDir()
	s := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, config.Secrets{})
	startOfferTestSupervisor(t, s)

	s.onOfferedConnectionSettings(offer(ts.endpoint("ws"), agentAuth), []byte("offer-1"))
	client := s.opampClient

	// The server offers the settings in use again, e.g. after a restart.
	s.onOfferedConnectionSettings(offer(ts.endpoint("ws"), agentAuth), []byte("offer-1-again"))
	assert.Same(t, client, s.opampClient, "settings already in use do not reconnect")
	require.Eventually(t, func() bool {
		return ts.hasStatus([]byte("offer-1-again"), protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLIED)
	}, 10*time.Second, 50*time.Millisecond)
}

func TestOfferedConnectionSettingsFromServer(t *testing.T) {
	// The whole path through opamp-go: the server sends a ConnectionSettingsOffers.
	enablePersistConnectionSettingsGate(t)
	ts := newOfferTestServer(t, bootstrapAuth, agentAuth)
	dir := t.TempDir()
	s := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, config.Secrets{})
	startOfferTestSupervisor(t, s)
	require.Eventually(t, func() bool { return len(ts.acceptedAuth()) > 0 }, 10*time.Second, 50*time.Millisecond)

	hash := []byte("server-offer")
	ts.send(t, &protobufs.ServerToAgent{
		InstanceUid: s.persistentState.InstanceID[:],
		ConnectionSettings: &protobufs.ConnectionSettingsOffers{
			Hash:  hash,
			Opamp: offer(ts.endpoint("ws"), agentAuth),
		},
	})

	require.Eventually(t, func() bool {
		return ts.hasStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLIED)
	}, 15*time.Second, 50*time.Millisecond)
	assert.True(t, ts.hasStatus(hash, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLYING),
		"APPLYING is reported while the new settings are verified")
	assert.True(t, fileExists(t, filepath.Join(dir, offeredConnectionSettingsFile)))
}

func TestOfferedConnectionSettingsBootstrapChanged(t *testing.T) {
	enablePersistConnectionSettingsGate(t)
	ts := newOfferTestServer(t, bootstrapAuth, agentAuth, "Bearer rotated")
	dir := t.TempDir()
	s := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, config.Secrets{})
	startOfferTestSupervisor(t, s)
	s.onOfferedConnectionSettings(offer(ts.endpoint("ws"), agentAuth), []byte("offer-1"))
	require.True(t, fileExists(t, filepath.Join(dir, offeredConnectionSettingsFile)))

	// The operator puts a new token in the config file.
	restarted := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, config.Secrets{})
	restarted.config.Server.Headers.Set("Authorization", "Bearer rotated")
	_, err := restarted.loadOfferedConnectionSettings()
	require.NoError(t, err)

	assert.Equal(t, "Bearer rotated", restarted.config.Server.Headers.Get("Authorization"), "the config file wins")
	assert.False(t, fileExists(t, filepath.Join(dir, offeredConnectionSettingsFile)))
	assert.Nil(t, restarted.persistentState.OpAMPConnectionSettings)
}

func TestOfferedConnectionSettingsPendingCandidate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		allowed    []string
		wantAuth   string
		wantStatus protobufs.ConnectionSettingsStatuses
		wantValid  bool
	}{
		{"accepted", []string{bootstrapAuth, agentAuth}, agentAuth, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_APPLIED, true},
		{"rejected", []string{bootstrapAuth}, bootstrapAuth, protobufs.ConnectionSettingsStatuses_ConnectionSettingsStatuses_FAILED, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enablePersistConnectionSettingsGate(t)
			setVerifyTimeout(t, 3*time.Second)
			ts := newOfferTestServer(t, tc.allowed...)
			dir := t.TempDir()

			// A Supervisor stopped while verifying an offer.
			stopped := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, config.Secrets{})
			_, err := stopped.loadOfferedConnectionSettings()
			require.NoError(t, err)
			hash := []byte("interrupted")
			require.NoError(t, stopped.saveConnectionSettingsCandidate(offer(ts.endpoint("ws"), agentAuth), hash))

			s := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, config.Secrets{})
			pending := startOfferTestSupervisor(t, s)
			require.NotNil(t, pending, "the candidate is verified again")
			s.offeredConnSettings.mu.Lock()
			s.finishPendingConnectionSettings(pending)

			assert.Equal(t, tc.wantAuth, s.config.Server.Headers.Get("Authorization"))
			assert.Equal(t, tc.wantValid, fileExists(t, filepath.Join(dir, offeredConnectionSettingsFile)))
			assert.False(t, fileExists(t, filepath.Join(dir, offeredConnectionSettingsCandidateFile)))
			require.Eventually(t, func() bool { return ts.hasStatus(hash, tc.wantStatus) }, 10*time.Second, 50*time.Millisecond)
		})
	}
}

func TestOfferedConnectionSettingsEncrypted(t *testing.T) {
	enablePersistConnectionSettingsGate(t)
	t.Setenv("TEST_OPAMP_SECRETS_KEY", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)))
	secrets := config.Secrets{KeySource: config.SecretsKeySourceEnv, KeyName: "TEST_OPAMP_SECRETS_KEY"}
	ts := newOfferTestServer(t, bootstrapAuth, agentAuth)
	dir := t.TempDir()
	s := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, secrets)
	startOfferTestSupervisor(t, s)

	s.onOfferedConnectionSettings(offer(ts.endpoint("ws"), agentAuth), []byte("offer-1"))

	raw, err := os.ReadFile(filepath.Join(dir, offeredConnectionSettingsFile))
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "agent-1", "the token is not stored in plain text")

	restarted := newOfferTestSupervisor(t, ts.endpoint("ws"), dir, secrets)
	_, err = restarted.loadOfferedConnectionSettings()
	require.NoError(t, err)
	assert.Equal(t, agentAuth, restarted.config.Server.Headers.Get("Authorization"))
}

func TestLoadOfferedConnectionSettingsDisabled(t *testing.T) {
	ts := newOfferTestServer(t, bootstrapAuth)
	s := newOfferTestSupervisor(t, ts.endpoint("ws"), t.TempDir(), config.Secrets{})
	pending, err := s.loadOfferedConnectionSettings()
	require.NoError(t, err)
	assert.Nil(t, pending)
	assert.Nil(t, s.offeredConnSettings, "without the feature gate, upstream behavior is unchanged")
}

func TestValidateOfferedConnectionSettingsConfig(t *testing.T) {
	status := config.Supervisor{Capabilities: config.Capabilities{ReportsConnectionSettingsStatus: true, AcceptsOpAMPConnectionSettings: true}}
	require.ErrorContains(t, validateOfferedConnectionSettingsConfig(status), "feature gate")

	secrets := config.Supervisor{Storage: config.Storage{Secrets: config.Secrets{KeySource: config.SecretsKeySourceEnv}}}
	require.ErrorContains(t, validateOfferedConnectionSettingsConfig(secrets), "feature gate")

	require.NoError(t, validateOfferedConnectionSettingsConfig(config.Supervisor{}))

	enablePersistConnectionSettingsGate(t)
	require.NoError(t, validateOfferedConnectionSettingsConfig(status))
	require.NoError(t, validateOfferedConnectionSettingsConfig(secrets))
	noAccept := config.Supervisor{Capabilities: config.Capabilities{ReportsConnectionSettingsStatus: true}}
	require.ErrorContains(t, validateOfferedConnectionSettingsConfig(noAccept), "requires capabilities::accepts_opamp_connection_settings")
}

func TestOpAMPServerHash(t *testing.T) {
	base := config.OpAMPServer{Endpoint: "wss://opamp.example.com/v1/opamp", Headers: http.Header{"Authorization": []string{"Bearer a"}}}
	h1, err := opampServerHash(base)
	require.NoError(t, err)
	again, err := opampServerHash(base)
	require.NoError(t, err)
	assert.Equal(t, h1, again)

	rotated := base
	rotated.Headers = http.Header{"Authorization": []string{"Bearer b"}}
	h2, err := opampServerHash(rotated)
	require.NoError(t, err)
	assert.NotEqual(t, h1, h2, "a new token in the config file changes the hash")

	withKey := base
	withKey.TLS.KeyPem = "key"
	h3, err := opampServerHash(withKey)
	require.NoError(t, err)
	assert.NotEqual(t, h1, h3, "secret TLS material is part of the hash")
}
