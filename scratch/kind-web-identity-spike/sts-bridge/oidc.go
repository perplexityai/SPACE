package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"
)

var b64 = base64.RawURLEncoding

// signer holds the issuer's RSA key; kid is derived from the public key, so
// it changes only with the key.
type signer struct {
	key *rsa.PrivateKey
	kid string
}

func loadSigner(path string) (*signer, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseSigner(raw)
}

func parseSigner(raw []byte) (*signer, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("no PEM block in the signing key")
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	rk, ok := k.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("signing key is not RSA")
	}
	der, err := x509.MarshalPKIXPublicKey(&rk.PublicKey)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(der)
	return &signer{key: rk, kid: hex.EncodeToString(sum[:8])}, nil
}

// mint signs claims as an RS256 JWT under the signer's kid.
func (s *signer) mint(claims map[string]any) (string, error) {
	h, err := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT", "kid": s.kid})
	if err != nil {
		return "", err
	}
	c, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	input := b64.EncodeToString(h) + "." + b64.EncodeToString(c)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return input + "." + b64.EncodeToString(sig), nil
}

// issuerClaims are what the bridge mints for a verified caller: short-lived,
// one subject, one audience, a unique jti.
func issuerClaims(iss, sub, aud string, now time.Time, ttl time.Duration) map[string]any {
	nonce := make([]byte, 12)
	_, _ = rand.Read(nonce)
	return map[string]any{
		"iss": iss,
		"sub": sub,
		"aud": aud,
		"iat": now.Unix(),
		"nbf": now.Unix(),
		"exp": now.Add(ttl).Unix(),
		"jti": hex.EncodeToString(nonce),
	}
}

// issuerHandler serves the OIDC discovery document and the JWKS: all
// versitygw fetches from an issuer.
func issuerHandler(iss, jwksBase string, s *signer) http.Handler {
	jwks, _ := json.Marshal(map[string]any{"keys": []any{map[string]string{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": s.kid,
		"n": b64.EncodeToString(s.key.N.Bytes()),
		"e": b64.EncodeToString(big.NewInt(int64(s.key.E)).Bytes()),
	}}})
	discovery, _ := json.Marshal(map[string]any{
		"issuer":                                iss,
		"jwks_uri":                              strings.TrimRight(jwksBase, "/") + "/jwks.json",
		"response_types_supported":              []string{"id_token"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
	})
	mux := http.NewServeMux()
	serve := func(body []byte) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		}
	}
	mux.Handle("GET /.well-known/openid-configuration", serve(discovery))
	mux.Handle("GET /jwks.json", serve(jwks))
	return mux
}
