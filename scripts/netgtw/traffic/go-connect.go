// Exercise the standard library HTTPS-proxy path, including its ALPN and CONNECT behavior.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"time"
)

type probe struct {
	Host        string  `json:"host"`
	Target      string  `json:"target"`
	Port        int     `json:"port"`
	TunnelHost  string  `json:"tunnel_host"`
	Certificate string  `json:"certificate"`
	Body        []byte  `json:"body"`
	Timeout     float64 `json:"timeout"`
}

type handshake struct {
	ALPN           string `json:"alpn"`
	VerifiedChains int    `json:"verified_chains"`
}

func main() {
	var input probe
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		panic(err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		panic(err)
	}
	pem, err := os.ReadFile(input.Certificate)
	if err != nil || !roots.AppendCertsFromPEM(pem) {
		panic("cannot load guest trust")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	// Resolve only the synthetic proxy endpoint; keep ordinary net/http HTTPS
	// proxy negotiation, CONNECT, authentication, and inner TLS behavior.
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != fmt.Sprintf("%s:%d", input.Host, input.Port) {
			return nil, fmt.Errorf("unexpected dial %s", address)
		}
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, fmt.Sprintf("%s:%d", input.Target, input.Port))
	}
	defer transport.CloseIdleConnections()
	result := map[string]any{"received": false, "protocol": "https-connect"}
	var handshakes []handshake
	trace := &httptrace.ClientTrace{TLSHandshakeDone: func(state tls.ConnectionState, _ error) {
		handshakes = append(handshakes, handshake{state.NegotiatedProtocol, len(state.VerifiedChains)})
	}}

	req, err := http.NewRequest("POST", "https://"+input.TunnelHost+"/echo", bytes.NewReader(input.Body))
	if err != nil {
		panic(err)
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	response, err := (&http.Client{Transport: transport, Timeout: time.Duration(input.Timeout * float64(time.Second))}).Do(req)
	if err != nil {
		result["error"] = err.Error()
	} else {
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		result["http_status"] = response.StatusCode
		result["bytes"] = len(body)
		result["received"] = readErr == nil && response.StatusCode == 200 && bytes.Equal(body, input.Body)
		if len(handshakes) != 2 {
			panic("expected proxy and inner TLS handshakes")
		}
		for _, handshake := range handshakes {
			if handshake.VerifiedChains == 0 {
				panic("TLS peer was not verified")
			}
		}
		result["downstream_alpn"] = handshakes[0].ALPN
		if readErr != nil {
			result["error"] = readErr.Error()
		}
	}
	result["tls_handshakes"] = handshakes
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		panic(err)
	}
}
