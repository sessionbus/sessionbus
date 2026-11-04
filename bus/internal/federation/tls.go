// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"bytes"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
)

const SecretBytes = 32

const (
	hostLabel = "sessionbus/v1/host"
	hubLabel  = "sessionbus/v1/hub"
)

func NewSecret() (string, error) {
	value := make([]byte, SecretBytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(value), nil
}

func ClientTLS(host, secret string) (*tls.Config, error) {
	hostKey, err := derive(secret, hostLabel)
	if err != nil {
		return nil, err
	}
	hubKey, _ := derive(secret, hubLabel)
	certificate, err := selfSigned(hostKey)
	if err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
		NextProtos: []string{TransferALPN, WakeALPN, TraceALPN, RosterALPN}, ServerName: host, InsecureSkipVerify: true, Certificates: []tls.Certificate{certificate},
		VerifyConnection: pin(hubKey.Public().(ed25519.PublicKey))}, nil
}

func ServerTLS(secrets map[string]string) (*tls.Config, error) {
	byHost := make(map[string]*tls.Config, len(secrets))
	seen := map[string]bool{}
	for host, secret := range secrets {
		decoded, decodeErr := base64.StdEncoding.Strict().DecodeString(secret)
		if !validHost(host) || decodeErr != nil || len(decoded) < SecretBytes {
			return nil, fmt.Errorf("invalid federation configuration for host %q", host)
		}
		if seen[string(decoded)] {
			return nil, errors.New("duplicate federation secret")
		}
		seen[string(decoded)] = true
		hostKey, err := derive(secret, hostLabel)
		if err != nil {
			return nil, fmt.Errorf("host %q: %w", host, err)
		}
		hubKey, _ := derive(secret, hubLabel)
		certificate, err := selfSigned(hubKey)
		if err != nil {
			return nil, err
		}
		byHost[host] = &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
			NextProtos: []string{TransferALPN, WakeALPN, TraceALPN, RosterALPN}, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAnyClientCert,
			VerifyConnection: pin(hostKey.Public().(ed25519.PublicKey))}
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			configuration := byHost[hello.ServerName]
			if hello.ServerName == "local" || configuration == nil {
				return nil, fmt.Errorf("unknown federation host %q", hello.ServerName)
			}
			return configuration, nil
		}}, nil
}

func derive(encoded, label string) (ed25519.PrivateKey, error) {
	secret, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(secret) < SecretBytes {
		return nil, errors.New("federation secret must be base64 for at least 32 bytes")
	}
	seed, err := hkdf.Key(sha256.New, secret, nil, label, ed25519.SeedSize)
	if err != nil {
		return nil, err
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

func selfSigned(key ed25519.PrivateKey) (tls.Certificate, error) {
	template := &x509.Certificate{SerialNumber: big.NewInt(1)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, err
}

func pin(expected ed25519.PublicKey) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("federation peer did not present a certificate")
		}
		actual, ok := state.PeerCertificates[0].PublicKey.(ed25519.PublicKey)
		if !ok || !bytes.Equal(actual, expected) {
			return errors.New("federation peer key mismatch")
		}
		return nil
	}
}
