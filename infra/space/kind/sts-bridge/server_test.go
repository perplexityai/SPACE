package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

// A client that stalls before the bridge authenticates anything is cut off
// within the listener's limits: one that never sends its TLS handshake, one
// that never finishes its headers, and one that declares a body it never
// sends, which the STS handler reads before checking the signature.
func TestStalledClientsAreClosed(t *testing.T) {
	limits := serverLimits{
		readHeader: 200 * time.Millisecond,
		read:       400 * time.Millisecond,
		write:      2 * time.Second,
		idle:       2 * time.Second,
		maxConns:   4,
	}
	b := &bridge{ids: vendIdentities(signedSecret), signer: testSigner(t), issuer: testIssuer,
		audience: testAudience, tokenTTL: time.Minute, upstream: "https://object-store-iam.invalid/",
		httpClient: http.DefaultClient}
	srv := httptest.NewUnstartedServer(b)
	srv.Config = newServer(b, limits)
	srv.Listener = limitListener(srv.Listener, limits.maxConns)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())

	// Generous next to the limits above, and far below "indefinitely".
	const bound = 3 * time.Second
	for _, tc := range []struct {
		name      string
		handshake bool
		send      string
	}{
		{name: "no TLS handshake"},
		{name: "headers never finished", handshake: true,
			send: "POST / HTTP/1.1\r\nHost: sts-bridge\r\n"},
		{name: "body never finished", handshake: true,
			send: "POST / HTTP/1.1\r\nHost: sts-bridge\r\nContent-Type: application/x-www-form-urlencoded\r\n" +
				"Content-Length: 4096\r\n\r\nAction=AssumeRole&Version=2011-06-15"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			raw, err := net.Dial("tcp", srv.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			conn := raw
			if tc.handshake {
				c := tls.Client(raw, &tls.Config{RootCAs: roots, ServerName: "example.com"})
				if err := c.Handshake(); err != nil {
					t.Fatal(err)
				}
				if _, err := io.WriteString(c, tc.send); err != nil {
					t.Fatal(err)
				}
				conn = c
			}
			_ = conn.SetReadDeadline(start.Add(bound))
			// Whatever the server answers, it must then close the
			// connection: a clean EOF or a reset, never the deadline.
			_, err = io.Copy(io.Discard, conn)
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("connection still open after %v", bound)
			}
			t.Logf("closed after %v (%v)", time.Since(start).Round(time.Millisecond), err)
		})
	}
}
