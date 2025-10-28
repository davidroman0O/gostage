package gostage

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	processproto "github.com/davidroman0O/gostage/v3/process/proto"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

func TestRemoteCoordinatorBindAdvertiseAddress(t *testing.T) {
	ctx := context.Background()
	queue := state.NewMemoryQueue()
	base := node.New(ctx, nil, node.TelemetryDispatcherConfig{})
	health := node.NewHealthDispatcher()

	binding := &poolBinding{
		pool: pools.NewLocal("remote", state.Selector{}, 1),
		remote: &remoteBinding{
			poolCfg: PoolConfig{Name: "remote", Slots: 1},
		},
	}

	dispatcher := newDispatcher(ctx, queue, nil, nil, nil, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, telemetry.NoopLogger{}, 0, 0, 0, nil, []*poolBinding{binding}, time.Now)
	rc, err := newRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, telemetry.NoopLogger{}, []*poolBinding{binding}, time.Now, RemoteBridgeConfig{
		BindAddress:      "127.0.0.1:0",
		AdvertiseAddress: "bridge.service",
	})
	if err != nil {
		t.Fatalf("newRemoteCoordinator: %v", err)
	}
	defer rc.shutdown()

	if !strings.HasPrefix(rc.bindAddress, "127.0.0.1:") {
		t.Fatalf("expected bind address on 127.0.0.1, got %s", rc.bindAddress)
	}
	_, port, err := net.SplitHostPort(rc.bindAddress)
	if err != nil {
		t.Fatalf("split bind address: %v", err)
	}
	wantAdvertise := "bridge.service"
	if port != "" {
		wantAdvertise = wantAdvertise + ":" + port
	}
	if rc.address != wantAdvertise {
		t.Fatalf("expected advertise address %s, got %s", wantAdvertise, rc.address)
	}
}

func TestRemoteCoordinatorTLSHandshake(t *testing.T) {
	ctx := context.Background()
	queue := state.NewMemoryQueue()
	base := node.New(ctx, nil, node.TelemetryDispatcherConfig{})
	health := node.NewHealthDispatcher()

	binding := &poolBinding{
		pool: pools.NewLocal("remote", state.Selector{}, 1),
		remote: &remoteBinding{
			poolCfg: PoolConfig{Name: "remote", Slots: 1},
		},
	}

	certPath, keyPath := writeTestCertificate(t, "server")

	dispatcher := newDispatcher(ctx, queue, nil, nil, nil, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, telemetry.NoopLogger{}, 0, 0, 0, nil, []*poolBinding{binding}, time.Now)
	rc, err := newRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, telemetry.NoopLogger{}, []*poolBinding{binding}, time.Now, RemoteBridgeConfig{
		BindAddress: "127.0.0.1:0",
		TLS: TLSFiles{
			CertPath: certPath,
			KeyPath:  keyPath,
			CAPath:   certPath,
		},
		RequireClientCert: true,
	})
	if err != nil {
		t.Fatalf("newRemoteCoordinator: %v", err)
	}
	defer rc.shutdown()

	dialCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	// Insecure dial should fail when TLS is required.
	connPlain, err := grpc.DialContext(dialCtx, rc.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial insecure connection: %v", err)
	}
	if _, err := processproto.NewProcessBridgeClient(connPlain).Control(dialCtx); err == nil {
		t.Fatalf("expected insecure control stream to fail")
	}
	_ = connPlain.Close()

	caPool := x509.NewCertPool()
	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("read cert: %v", err)
	}
	if !caPool.AppendCertsFromPEM(pemBytes) {
		t.Fatalf("append cert to pool")
	}

	// Dial without client cert should fail due to mutual TLS requirement.
	connNoClient, err := grpc.DialContext(dialCtx, rc.address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs:    caPool,
		MinVersion: tls.VersionTLS12,
	})))
	if err != nil {
		t.Fatalf("dial without client cert: %v", err)
	}
	if _, err := processproto.NewProcessBridgeClient(connNoClient).Control(dialCtx); err == nil {
		t.Fatalf("expected mutual TLS control stream to fail without client cert")
	}
	_ = connNoClient.Close()

	clientCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load client cert: %v", err)
	}

	conn, err := grpc.DialContext(dialCtx, rc.address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	})))
	if err != nil {
		t.Fatalf("mutual TLS dial failed: %v", err)
	}
	_ = conn.Close()
}

func TestRemoteCoordinatorTokenValidation(t *testing.T) {
	ctx := context.Background()
	queue := state.NewMemoryQueue()
	base := node.New(ctx, nil, node.TelemetryDispatcherConfig{})
	health := node.NewHealthDispatcher()
	diag := &diagCollector{}

	binding := &poolBinding{
		pool: pools.NewLocal("remote", state.Selector{}, 1),
		remote: &remoteBinding{
			poolCfg: PoolConfig{Name: "remote", Slots: 1},
		},
	}

	dispatcher := newDispatcher(ctx, queue, nil, nil, nil, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, 0, 0, 0, nil, []*poolBinding{binding}, time.Now)
	rc, err := newRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, []*poolBinding{binding}, time.Now, RemoteBridgeConfig{})
	if err != nil {
		t.Fatalf("newRemoteCoordinator: %v", err)
	}
	defer rc.shutdown()

	sp := &spawnerBinding{name: "remote-spawner", cfg: SpawnerConfig{Name: "remote-spawner", AuthToken: "secret"}}
	binding.remote.spawner = sp
	if pool := rc.pools["remote"]; pool != nil {
		pool.binding.remote.spawner = sp
	}

	dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, rc.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := processproto.NewProcessBridgeClient(conn).Control(dialCtx)
	if err != nil {
		t.Fatalf("control stream: %v", err)
	}
	defer stream.CloseSend()

	register := &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_Register{Register: &processproto.RegisterNode{
		NodeId:    "child-1",
		ChildType: "remote",
		Pools:     []*processproto.ChildPool{{Name: "remote", Slots: 1}},
		AuthToken: "wrong",
	}}}
	if err := stream.Send(register); err != nil {
		t.Fatalf("send register: %v", err)
	}

	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied, got %v", err)
	}

	events := diag.Events()
	if len(events) == 0 {
		t.Fatalf("expected diagnostics for invalid token")
	}

	connOK, err := grpc.DialContext(dialCtx, rc.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial (valid): %v", err)
	}
	defer connOK.Close()

	streamOK, err := processproto.NewProcessBridgeClient(connOK).Control(dialCtx)
	if err != nil {
		t.Fatalf("control stream (valid): %v", err)
	}
	defer streamOK.CloseSend()

	registerOK := &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_Register{Register: &processproto.RegisterNode{
		NodeId:    "child-2",
		ChildType: "remote",
		Pools:     []*processproto.ChildPool{{Name: "remote", Slots: 1}},
		AuthToken: "secret",
	}}}
	if err := streamOK.Send(registerOK); err != nil {
		t.Fatalf("send register (valid): %v", err)
	}
	ack, err := streamOK.Recv()
	if err != nil {
		t.Fatalf("recv ack (valid): %v", err)
	}
	if ack.GetRegisterAck() == nil {
		t.Fatalf("expected register ack, got %T", ack.GetBody())
	}
}

func writeTestCertificate(t *testing.T, name string) (certPath, keyPath string) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "sandbox"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")

	certFile, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("encode cert: %v", err)
	}
	_ = certFile.Close()

	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	keyBytes := x509.MarshalPKCS1PrivateKey(priv)
	if err := pem.Encode(keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyBytes}); err != nil {
		t.Fatalf("encode key: %v", err)
	}
	_ = keyFile.Close()

	return certPath, keyPath
}
