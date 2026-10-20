package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// An AssumeRole request as the aws CLI (botocore) signed it, with synthetic
// keys: the oracle is AWS's own signer, not this package's canonicalization.
const (
	signedAccessKey = "AKIDEXAMPLEVEND"
	signedSecret    = "example-vend-secret-for-tests"
	signedHost      = "127.0.0.1:18081"
	signedDate      = "20261016T155916Z"
	signedAuth      = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLEVEND/20261016/us-east-1/sts/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=4d94921370fc9dff962d06db31bd97481ae59c5fa414582557245e868b65604d"
	signedType      = "application/x-www-form-urlencoded; charset=utf-8"
	signedBody      = "Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Fkind-storage-vend&RoleSessionName=space-storage-exchange&Policy=%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%7B%22Effect%22%3A%22Allow%22%2C%22Action%22%3A%5B%22s3%3AGetObject%22%5D%2C%22Resource%22%3A%5B%22arn%3Aaws%3As3%3A%3A%3Asnapshots%2Fsuspend_snapshots%2F00%2Fa%2Fb%22%5D%7D%5D%7D&DurationSeconds=3600"
)

var signedAt, _ = time.Parse(amzDateForm, signedDate)

func signedRequest() (*http.Request, []byte) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(signedBody))
	r.Host = signedHost
	r.Header.Set("Content-Type", signedType)
	r.Header.Set("X-Amz-Date", signedDate)
	r.Header.Set("Authorization", signedAuth)
	return r, []byte(signedBody)
}

func vendIdentities(secret string) map[string]identity {
	return map[string]identity{signedAccessKey: {AccessKey: signedAccessKey, Secret: secret, Subject: "kind-storage-vend"}}
}

func TestVerifySigV4AcceptsAWSSignedRequest(t *testing.T) {
	r, body := signedRequest()
	id, err := verifySigV4(r, body, vendIdentities(signedSecret), signedAt.Add(time.Minute))
	if err != nil {
		t.Fatalf("refused a request botocore signed: %v", err)
	}
	if id.Subject != "kind-storage-vend" {
		t.Fatalf("subject %q, want kind-storage-vend", id.Subject)
	}
}

// The refusals the bridge's B2-B8 checks cover, each a single change to the
// signed request.
func TestVerifySigV4Refusals(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(r *http.Request, body []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time)
		status int
		code   string
	}{
		{"B2 no Authorization header", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Header.Del("Authorization")
			return b, ids, signedAt
		}, 403, "MissingAuthenticationToken"},
		{"B3 wrong secret", func(r *http.Request, b []byte, _ map[string]identity) ([]byte, map[string]identity, time.Time) {
			return b, vendIdentities("another-secret"), signedAt
		}, 403, "SignatureDoesNotMatch"},
		{"B4 unknown access key", func(r *http.Request, b []byte, _ map[string]identity) ([]byte, map[string]identity, time.Time) {
			return b, map[string]identity{"AKIDOTHER": {AccessKey: "AKIDOTHER", Secret: signedSecret, Subject: "x"}}, signedAt
		}, 403, "InvalidClientTokenId"},
		{"B5 body changed after signing", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			return []byte(strings.Replace(string(b), "DurationSeconds=3600", "DurationSeconds=3599", 1)), ids, signedAt
		}, 403, "SignatureDoesNotMatch"},
		{"B6 X-Amz-Date 16 minutes old", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			return b, ids, signedAt.Add(16 * time.Minute)
		}, 403, "SignatureDoesNotMatch"},
		{"B6 X-Amz-Date 16 minutes ahead", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			return b, ids, signedAt.Add(-16 * time.Minute)
		}, 403, "SignatureDoesNotMatch"},
		{"B7 session token beside the vend keys", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Header.Set("X-Amz-Security-Token", "session-token")
			return b, ids, signedAt
		}, 403, "InvalidClientTokenId"},
		{"B8 a vended session's keys", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Header.Set("Authorization", strings.Replace(signedAuth, signedAccessKey, "ASIAVENDEDSESSION", 1))
			r.Header.Set("X-Amz-Security-Token", "session-token")
			return b, ids, signedAt
		}, 403, "InvalidClientTokenId"},
		{"signed header value changed", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			return b, ids, signedAt
		}, 403, "SignatureDoesNotMatch"},
		{"host changed", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Host = "sts-bridge:8080"
			return b, ids, signedAt
		}, 403, "SignatureDoesNotMatch"},
		{"scope date differs from X-Amz-Date", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Header.Set("Authorization", strings.Replace(signedAuth, "/20261016/", "/20261015/", 1))
			return b, ids, signedAt
		}, 403, "SignatureDoesNotMatch"},
		{"scope names another service", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Header.Set("Authorization", strings.Replace(signedAuth, "/sts/", "/s3/", 1))
			return b, ids, signedAt
		}, 403, "SignatureDoesNotMatch"},
		{"x-amz-date not signed", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Header.Set("Authorization", strings.Replace(signedAuth, "content-type;host;x-amz-date", "content-type;host", 1))
			return b, ids, signedAt
		}, 403, "SignatureDoesNotMatch"},
		{"payload hash header disagrees with the body", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Header.Set("X-Amz-Content-Sha256", sha256Hex([]byte("other")))
			return b, ids, signedAt
		}, 403, "SignatureDoesNotMatch"},
		{"presigned query", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Header.Del("Authorization")
			r.URL.RawQuery = "X-Amz-Algorithm=AWS4-HMAC-SHA256"
			return b, ids, signedAt
		}, 403, "AccessDenied"},
		{"another algorithm", func(r *http.Request, b []byte, ids map[string]identity) ([]byte, map[string]identity, time.Time) {
			r.Header.Set("Authorization", strings.Replace(signedAuth, "AWS4-HMAC-SHA256", "AWS4-ECDSA-P256-SHA256", 1))
			return b, ids, signedAt
		}, 400, "IncompleteSignature"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, body := signedRequest()
			body, ids, now := tc.mutate(r, body, vendIdentities(signedSecret))
			_, err := verifySigV4(r, body, ids, now)
			if err == nil {
				t.Fatal("accepted")
			}
			if err.Status != tc.status || err.Code != tc.code {
				t.Fatalf("got %d %s (%s), want %d %s", err.Status, err.Code, err.Message, tc.status, tc.code)
			}
		})
	}
}
