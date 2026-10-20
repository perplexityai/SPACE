package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

// serverLimits bound what one client can hold on a listener. Both listeners
// answer before authenticating anything (the STS handler reads the body to
// check its signature), so without them a client that never finishes its
// headers or body holds a connection and a goroutine indefinitely.
type serverLimits struct {
	// readHeader bounds the TLS handshake too: net/http applies the
	// smallest of its read and write timeouts to it.
	readHeader time.Duration
	// read covers the whole request, the at most 64 KiB body included.
	read time.Duration
	// write runs from the end of the headers to the end of the response, so
	// it outlasts read plus the 10 s budget of the upstream exchange.
	write time.Duration
	// idle outlasts the AWS SDK's 90 s connection pool idle timeout (the
	// hyper-util default the authority's client keeps), so the client
	// retires a kept-alive connection before the bridge closes it.
	idle time.Duration
	// maxConns caps open connections; further ones wait to be accepted.
	maxConns int
}

var defaultLimits = serverLimits{
	readHeader: 5 * time.Second,
	read:       10 * time.Second,
	write:      30 * time.Second,
	idle:       2 * time.Minute,
	maxConns:   128,
}

func newServer(h http.Handler, l serverLimits) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: l.readHeader,
		ReadTimeout:       l.read,
		WriteTimeout:      l.write,
		IdleTimeout:       l.idle,
		MaxHeaderBytes:    16 << 10,
	}
}

// serveTLS serves h over TLS on addr within l until the listener fails.
func serveTLS(addr, certFile, keyFile string, h http.Handler, l serverLimits) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return newServer(h, l).ServeTLS(limitListener(ln, l.maxConns), certFile, keyFile)
}

// limitListener accepts at most n connections at a time, as
// golang.org/x/net/netutil.LimitListener does, which this standard-library
// module does not import.
func limitListener(ln net.Listener, n int) net.Listener {
	return &limitedListener{Listener: ln, slots: make(chan struct{}, n)}
}

type limitedListener struct {
	net.Listener
	slots chan struct{}
}

func (l *limitedListener) Accept() (net.Conn, error) {
	l.slots <- struct{}{}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &limitedConn{Conn: c, release: func() { <-l.slots }}, nil
}

type limitedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// upstreamClient is the client for versitygw's IAM service: HTTPS only,
// verified against the CA bundle in caFile, since the minted token and the
// session's secrets cross this hop.
func upstreamClient(upstream, caFile string) (*http.Client, error) {
	u, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("-upstream: %w", err)
	}
	if u.Scheme != "https" {
		return nil, fmt.Errorf("-upstream must be an https URL, not %q", upstream)
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("-upstream-ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("-upstream-ca holds no PEM certificate")
	}
	return &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool},
			TLSHandshakeTimeout: 5 * time.Second,
			IdleConnTimeout:     90 * time.Second,
			MaxIdleConnsPerHost: 8,
		},
	}, nil
}
