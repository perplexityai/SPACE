package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

const (
	testIssuer   = "https://sts-bridge.kind.invalid"
	testAudience = "kind-sts-bridge"
)

func testSigner(t *testing.T) *signer {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	s, err := parseSigner(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// verifyToken checks an RS256 JWT against the key the issuer's JWKS
// publishes, as versitygw does, and returns its header and claims.
func verifyToken(t *testing.T, jwks []byte, token string) (map[string]any, map[string]any) {
	t.Helper()
	var set struct {
		Keys []struct{ Kid, N, E string } `json:"keys"`
	}
	if err := json.Unmarshal(jwks, &set); err != nil || len(set.Keys) != 1 {
		t.Fatalf("JWKS %s: %v", jwks, err)
	}
	n, _ := b64.DecodeString(set.Keys[0].N)
	e, _ := b64.DecodeString(set.Keys[0].E)
	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts", len(parts))
	}
	sig, _ := b64.DecodeString(parts[2])
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("signature does not verify against the JWKS: %v", err)
	}
	var header, claims map[string]any
	h, _ := b64.DecodeString(parts[0])
	c, _ := b64.DecodeString(parts[1])
	if json.Unmarshal(h, &header) != nil || json.Unmarshal(c, &claims) != nil {
		t.Fatal("token header or claims are not JSON")
	}
	if header["kid"] != set.Keys[0].Kid || header["alg"] != "RS256" {
		t.Fatalf("header %v does not name the published key", header)
	}
	return header, claims
}

func fetch(t *testing.T, h http.Handler, path string) []byte {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != 200 {
		t.Fatalf("GET %s: %d", path, rec.Code)
	}
	return rec.Body.Bytes()
}

func TestIssuerPublishesTheSigningKey(t *testing.T) {
	s := testSigner(t)
	h := issuerHandler(testIssuer, "https://sts-bridge:8443/", s)
	var discovery map[string]any
	if err := json.Unmarshal(fetch(t, h, "/.well-known/openid-configuration"), &discovery); err != nil {
		t.Fatal(err)
	}
	if discovery["issuer"] != testIssuer || discovery["jwks_uri"] != "https://sts-bridge:8443/jwks.json" {
		t.Fatalf("discovery %v", discovery)
	}
	now := time.Now()
	token, err := s.mint(issuerClaims(testIssuer, "kind-storage-vend", testAudience, now, 5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, claims := verifyToken(t, fetch(t, h, "/jwks.json"), token)
	for k, want := range map[string]any{"iss": testIssuer, "sub": "kind-storage-vend", "aud": testAudience,
		"iat": float64(now.Unix()), "exp": float64(now.Add(5 * time.Minute).Unix())} {
		if claims[k] != want {
			t.Errorf("claim %s = %v, want %v", k, claims[k], want)
		}
	}
	other, _ := s.mint(issuerClaims(testIssuer, "kind-storage-vend", testAudience, now, time.Minute))
	_, otherClaims := verifyToken(t, fetch(t, h, "/jwks.json"), other)
	if otherClaims["jti"] == claims["jti"] {
		t.Error("two tokens share a jti")
	}
}

// fakeIAM records the form versitygw's AssumeRoleWithWebIdentity would get
// and answers with a canned status and body.
type fakeIAM struct {
	form   url.Values
	status int
	body   string
}

func (f *fakeIAM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	f.form, _ = url.ParseQuery(string(raw))
	w.WriteHeader(f.status)
	_, _ = io.WriteString(w, f.body)
}

const webIdentityOK = `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult>
<Credentials><AccessKeyId>ASIASESSION</AccessKeyId><SecretAccessKey>session-secret</SecretAccessKey><SessionToken>session-token</SessionToken><Expiration>2026-10-16T16:59:16Z</Expiration></Credentials>
<AssumedRoleUser><AssumedRoleId>AROAVEND:space-storage-exchange</AssumedRoleId><Arn>arn:aws:sts::000000000000:assumed-role/kind-storage-vend/space-storage-exchange</Arn></AssumedRoleUser>
<PackedPolicySize>12</PackedPolicySize></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`

func serveSigned(t *testing.T, iam *fakeIAM, edit func(r *http.Request, body string) (*http.Request, string)) (*httptest.ResponseRecorder, *signer) {
	t.Helper()
	upstream := httptest.NewServer(iam)
	t.Cleanup(upstream.Close)
	s := testSigner(t)
	b := &bridge{ids: vendIdentities(signedSecret), signer: s, issuer: testIssuer, audience: testAudience,
		tokenTTL: 5 * time.Minute, upstream: upstream.URL, httpClient: upstream.Client(),
		now: func() time.Time { return signedAt }}
	r, body := signedRequest()
	if edit != nil {
		var newBody string
		r, newBody = edit(r, string(body))
		body = []byte(newBody)
	}
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	b.ServeHTTP(rec, r)
	return rec, s
}

func TestAssumeRoleBecomesWebIdentity(t *testing.T) {
	iam := &fakeIAM{status: 200, body: webIdentityOK}
	rec, s := serveSigned(t, iam, nil)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	want, _ := url.ParseQuery(signedBody)
	if iam.form.Get("Action") != "AssumeRoleWithWebIdentity" || iam.form.Get("Version") != "2011-06-15" {
		t.Fatalf("upstream action %v", iam.form)
	}
	for _, k := range []string{"RoleArn", "RoleSessionName", "Policy", "DurationSeconds"} {
		if iam.form.Get(k) != want.Get(k) {
			t.Errorf("%s forwarded as %q, want %q", k, iam.form.Get(k), want.Get(k))
		}
	}
	_, claims := verifyToken(t, fetch(t, issuerHandler(testIssuer, testIssuer, s), "/jwks.json"), iam.form.Get("WebIdentityToken"))
	if claims["sub"] != "kind-storage-vend" || claims["aud"] != testAudience || claims["iss"] != testIssuer {
		t.Fatalf("minted claims %v", claims)
	}

	// The SDK's AssumeRole parser reads these elements.
	var out struct {
		XMLName xml.Name `xml:"https://sts.amazonaws.com/doc/2011-06-15/ AssumeRoleResponse"`
		Result  struct {
			Credentials struct {
				AccessKeyID, SecretAccessKey, SessionToken, Expiration string
			} `xml:"Credentials"`
			User struct{ AssumedRoleID, Arn string } `xml:"AssumedRoleUser"`
		} `xml:"AssumeRoleResult"`
	}
	if err := xml.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not AssumeRoleResponse: %v\n%s", err, rec.Body)
	}
	c := out.Result.Credentials
	if c.SecretAccessKey != "session-secret" || c.SessionToken != "session-token" || c.Expiration != "2026-10-16T16:59:16Z" {
		t.Fatalf("credentials %+v", c)
	}
	if !strings.HasSuffix(out.Result.User.Arn, "assumed-role/kind-storage-vend/space-storage-exchange") {
		t.Fatalf("assumed role %+v", out.Result.User)
	}
}

func TestUpstreamRefusalsKeepAssumeRoleMeaning(t *testing.T) {
	errBody := func(code string) string {
		return `<ErrorResponse><Error><Type>Sender</Type><Code>` + code + `</Code><Message>m</Message></Error></ErrorResponse>`
	}
	cases := []struct {
		upstreamStatus int
		upstreamCode   string
		status         int
		code           string
	}{
		// A token versitygw refuses means the role does not trust this caller.
		{400, "InvalidIdentityToken", 403, "AccessDenied"},
		{400, "ExpiredTokenException", 403, "AccessDenied"},
		{400, "IDPCommunicationError", 503, "ServiceUnavailable"},
		{403, "AccessDenied", 403, "AccessDenied"},
		{400, "ValidationError", 400, "ValidationError"},
	}
	for _, tc := range cases {
		t.Run(tc.upstreamCode, func(t *testing.T) {
			rec, _ := serveSigned(t, &fakeIAM{status: tc.upstreamStatus, body: errBody(tc.upstreamCode)}, nil)
			var out struct {
				Error struct{ Code string } `xml:"Error"`
			}
			if err := xml.Unmarshal(rec.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			if rec.Code != tc.status || out.Error.Code != tc.code {
				t.Fatalf("got %d %s, want %d %s", rec.Code, out.Error.Code, tc.status, tc.code)
			}
		})
	}
	rec, _ := serveSigned(t, &fakeIAM{status: 500, body: "not xml"}, nil)
	if rec.Code != 502 {
		t.Fatalf("unparseable upstream answer: %d, want 502", rec.Code)
	}
}

// Parameters the bridge cannot pass on are refused, never dropped: the
// session would otherwise be wider than the caller asked for. The body is
// changed before the request is signed again, so the check is on the
// parameter, not the signature.
func TestUnsupportedParametersAreRefused(t *testing.T) {
	r, _ := signedRequest()
	resign(r, signedBody)
	if r.Header.Get("Authorization") != signedAuth {
		t.Fatal("resign does not reproduce botocore's signature")
	}
	for _, extra := range []string{"ExternalId=x", "Tags.member.1.Key=k", "PolicyArns.member.1.arn=a", "SerialNumber=s"} {
		t.Run(extra, func(t *testing.T) {
			iam := &fakeIAM{status: 200, body: webIdentityOK}
			rec, _ := serveSigned(t, iam, func(r *http.Request, body string) (*http.Request, string) {
				body += "&" + extra
				resign(r, body)
				return r, body
			})
			if rec.Code != 400 || !strings.Contains(rec.Body.String(), "InvalidParameterValue") {
				t.Fatalf("got %d %s", rec.Code, rec.Body)
			}
			if iam.form != nil {
				t.Fatal("the refused request reached versitygw")
			}
		})
	}
}

// resign recomputes the signature of the fixture's signed headers for a new
// body. It reuses verifySigV4's canonicalization, so it is only for tests
// whose subject is not the signature.
func resign(r *http.Request, body string) {
	canon := "POST\n/\n\ncontent-type:" + signedType + "\nhost:" + signedHost + "\nx-amz-date:" + signedDate +
		"\n\ncontent-type;host;x-amz-date\n" + sha256Hex([]byte(body))
	toSign := sigAlgorithm + "\n" + signedDate + "\n20261016/us-east-1/sts/aws4_request\n" + sha256Hex([]byte(canon))
	key := hmacSHA256([]byte("AWS4"+signedSecret), "20261016")
	for _, s := range []string{"us-east-1", "sts", "aws4_request"} {
		key = hmacSHA256(key, s)
	}
	sig := hex.EncodeToString(hmacSHA256(key, toSign))
	r.Header.Set("Authorization", signedAuth[:strings.LastIndex(signedAuth, "=")+1]+sig)
}
