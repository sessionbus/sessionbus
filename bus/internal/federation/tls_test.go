// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"net"
	"strings"
	"testing"
	"time"
)

func TestSecretAndLabels(t *testing.T) {
	encoded, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	secret, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(secret) != SecretBytes {
		t.Fatalf("secret = %q, %v", encoded, err)
	}
	host, err := derive(encoded, hostLabel)
	if err != nil {
		t.Fatal(err)
	}
	hub, err := derive(encoded, hubLabel)
	if err != nil {
		t.Fatal(err)
	}
	hostAgain, _ := derive(encoded, hostLabel)
	if string(host) != string(hostAgain) || string(host.Public().(ed25519.PublicKey)) == string(hub.Public().(ed25519.PublicKey)) {
		t.Fatal("HKDF labels are not stable and distinct")
	}
	for _, invalid := range []string{"", base64.StdEncoding.EncodeToString(make([]byte, SecretBytes-1)), "not base64"} {
		if _, err := derive(invalid, hostLabel); err == nil {
			t.Fatalf("accepted secret %q", invalid)
		}
	}
	if _, err := derive(base64.StdEncoding.EncodeToString(make([]byte, SecretBytes+1)), hostLabel); err != nil {
		t.Fatalf("long secret: %v", err)
	}
}

func TestPinUsesOnlyLeafKey(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString(make([]byte, SecretBytes))
	key, _ := derive(encoded, hostLabel)
	certificate, err := selfSigned(key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := pin(key.Public().(ed25519.PublicKey))(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf, leaf}}); err != nil {
		t.Fatalf("extra chain certificate changed pin result: %v", err)
	}
}

func TestPinnedTLS(t *testing.T) {
	secret := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	server, err := ServerTLS(map[string]string{"alpha": secret})
	if err != nil {
		t.Fatal(err)
	}
	client, err := ClientTLS("alpha", secret)
	if err != nil {
		t.Fatal(err)
	}
	clientState, serverState, clientErr, serverErr := handshake(client, server)
	if clientErr != nil || serverErr != nil {
		t.Fatalf("handshake = client %v, server %v", clientErr, serverErr)
	}
	if clientState.Version != tls.VersionTLS13 || serverState.Version != tls.VersionTLS13 {
		t.Fatalf("versions = %x, %x", clientState.Version, serverState.Version)
	}
	if clientState.NegotiatedProtocol != TransferALPN || serverState.NegotiatedProtocol != TransferALPN {
		t.Fatalf("protocols = %q, %q", clientState.NegotiatedProtocol, serverState.NegotiatedProtocol)
	}
}

func TestTraceALPNFallsBackToRoster(t *testing.T) {
	secret := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	server, _ := ServerTLS(map[string]string{"alpha": secret})
	client, _ := ClientTLS("alpha", secret)
	client.NextProtos = []string{RosterALPN}
	clientState, serverState, clientErr, serverErr := handshake(client, server)
	if clientErr != nil || serverErr != nil || clientState.NegotiatedProtocol != RosterALPN || serverState.NegotiatedProtocol != RosterALPN {
		t.Fatalf("roster fallback = %q/%q, %v/%v", clientState.NegotiatedProtocol, serverState.NegotiatedProtocol, clientErr, serverErr)
	}
}

func TestServerTLSRejectsInvalidConfiguration(t *testing.T) {
	secret := base64.StdEncoding.EncodeToString(make([]byte, SecretBytes))
	for _, secrets := range []map[string]string{
		{"local": secret},
		{"Bad": secret},
		{"alpha": secret, "beta": secret},
	} {
		if _, err := ServerTLS(secrets); err == nil {
			t.Fatalf("accepted configuration %#v", secrets)
		}
	}
}

func TestPinnedTLSRejectsWrongIdentity(t *testing.T) {
	alpha := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	wrong := base64.StdEncoding.EncodeToString([]byte("fedcba9876543210fedcba9876543210"))
	server, _ := ServerTLS(map[string]string{"alpha": alpha})
	for _, test := range []struct {
		name, host, secret string
	}{
		{"wrong secret", "alpha", wrong},
		{"unknown host", "beta", alpha},
		{"local host", "local", alpha},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := ClientTLS(test.host, test.secret)
			if err != nil {
				t.Fatal(err)
			}
			_, _, clientErr, serverErr := handshake(client, server)
			if clientErr == nil || serverErr == nil {
				t.Fatalf("handshake errors = client %v, server %v", clientErr, serverErr)
			}
			if test.host != "alpha" && !strings.Contains(serverErr.Error(), "unknown federation host") {
				t.Fatalf("server error = %v", serverErr)
			}
		})
	}
}

func handshake(clientConfig, serverConfig *tls.Config) (tls.ConnectionState, tls.ConnectionState, error, error) {
	clientFD, serverFD := net.Pipe()
	deadline := time.Now().Add(2 * time.Second)
	_ = clientFD.SetDeadline(deadline)
	_ = serverFD.SetDeadline(deadline)
	client := tls.Client(clientFD, clientConfig)
	server := tls.Server(serverFD, serverConfig)
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.Handshake() }()
	clientErr := client.Handshake()
	serverErr := <-serverResult
	_ = client.Close()
	_ = server.Close()
	return client.ConnectionState(), server.ConnectionState(), clientErr, serverErr
}
