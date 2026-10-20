package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// identity is one long-term key pair the bridge accepts, and the OIDC
// subject it mints for that key.
type identity struct {
	AccessKey string
	Secret    string
	Subject   string
}

// authError is a refusal in STS's error vocabulary.
type authError struct {
	Status  int
	Code    string
	Message string
}

func (e *authError) Error() string { return e.Code + ": " + e.Message }

func refuse(status int, code, format string, args ...any) *authError {
	return &authError{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

const (
	sigAlgorithm = "AWS4-HMAC-SHA256"
	amzDateForm  = "20060102T150405Z"
	maxSkew      = 15 * time.Minute
)

// verifySigV4 checks a header-signed SigV4 request whose body has already
// been read, and returns the identity that signed it.
//
// Covered: the algorithm, the credential scope (date, region present,
// service sts, aws4_request), x-amz-date within maxSkew and matching the
// scope's date, the method, path, query string, every signed header (host
// and x-amz-date must be among them), and the SHA-256 of the body actually
// received. A session token is refused: vend identities hold long-term keys.
// Not covered: presigned (query-string) requests, SigV4A, chunked payload
// signing, and replay of a captured request inside the skew window, which
// AWS also accepts.
func verifySigV4(r *http.Request, body []byte, ids map[string]identity, now time.Time) (identity, *authError) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		if r.URL.Query().Get("X-Amz-Algorithm") != "" {
			return identity{}, refuse(403, "AccessDenied", "presigned requests are not accepted")
		}
		return identity{}, refuse(403, "MissingAuthenticationToken", "Request is missing Authentication Token")
	}
	alg, rest, _ := strings.Cut(auth, " ")
	if alg != sigAlgorithm {
		return identity{}, refuse(400, "IncompleteSignature", "unsupported signing algorithm")
	}
	fields := map[string]string{}
	for _, part := range strings.Split(rest, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok {
			fields[k] = v
		}
	}
	cred, signedHeaders, signature := fields["Credential"], fields["SignedHeaders"], fields["Signature"]
	if cred == "" || signedHeaders == "" || signature == "" {
		return identity{}, refuse(400, "IncompleteSignature", "Authorization header requires Credential, SignedHeaders and Signature")
	}
	scope := strings.Split(cred, "/")
	if len(scope) != 5 || scope[3] != "sts" || scope[4] != "aws4_request" || scope[2] == "" {
		return identity{}, refuse(403, "SignatureDoesNotMatch", "credential scope must be <key>/<date>/<region>/sts/aws4_request")
	}
	id, ok := ids[scope[0]]
	if !ok {
		return identity{}, refuse(403, "InvalidClientTokenId", "The security token included in the request is invalid.")
	}
	if r.Header.Get("X-Amz-Security-Token") != "" {
		return identity{}, refuse(403, "InvalidClientTokenId", "session credentials are not accepted here")
	}
	amzDate := r.Header.Get("X-Amz-Date")
	signedAt, err := time.Parse(amzDateForm, amzDate)
	if err != nil {
		return identity{}, refuse(400, "IncompleteSignature", "X-Amz-Date is missing or malformed")
	}
	if skew := now.Sub(signedAt); skew > maxSkew || skew < -maxSkew {
		return identity{}, refuse(403, "SignatureDoesNotMatch", "Signature expired: %s is outside the %s window around %s", amzDate, maxSkew, now.UTC().Format(amzDateForm))
	}
	if scope[1] != amzDate[:8] {
		return identity{}, refuse(403, "SignatureDoesNotMatch", "credential scope date does not match X-Amz-Date")
	}
	names := strings.Split(signedHeaders, ";")
	if !sort.StringsAreSorted(names) || !contains(names, "host") || !contains(names, "x-amz-date") {
		return identity{}, refuse(403, "SignatureDoesNotMatch", "SignedHeaders must be sorted and include host and x-amz-date")
	}
	bodyHash := sha256Hex(body)
	if h := r.Header.Get("X-Amz-Content-Sha256"); h != "" && h != bodyHash {
		return identity{}, refuse(403, "SignatureDoesNotMatch", "x-amz-content-sha256 does not match the body")
	}

	var canon strings.Builder
	canon.WriteString(r.Method + "\n")
	canon.WriteString(canonicalPath(r.URL.EscapedPath()) + "\n")
	canon.WriteString(canonicalQuery(r.URL.Query()) + "\n")
	for _, name := range names {
		var value string
		if name == "host" {
			value = r.Host
		} else {
			value = strings.Join(trimAll(r.Header.Values(name)), ",")
		}
		canon.WriteString(name + ":" + value + "\n")
	}
	canon.WriteString("\n" + signedHeaders + "\n" + bodyHash)

	credScope := strings.Join(scope[1:], "/")
	toSign := sigAlgorithm + "\n" + amzDate + "\n" + credScope + "\n" + sha256Hex([]byte(canon.String()))
	key := hmacSHA256([]byte("AWS4"+id.Secret), scope[1])
	for _, s := range scope[2:] {
		key = hmacSHA256(key, s)
	}
	want := hex.EncodeToString(hmacSHA256(key, toSign))
	if !hmac.Equal([]byte(want), []byte(signature)) {
		return identity{}, refuse(403, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.")
	}
	return id, nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// trimAll trims each header value and collapses inner runs of spaces, as
// SigV4's canonical headers require.
func trimAll(values []string) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = strings.Join(strings.Fields(v), " ")
	}
	return out
}

// canonicalPath re-encodes each segment once more, as SigV4 does for every
// service but S3; STS is only ever called on "/".
func canonicalPath(escaped string) string {
	if escaped == "" {
		return "/"
	}
	segs := strings.Split(escaped, "/")
	for i, s := range segs {
		segs[i] = awsEscape(s)
	}
	return strings.Join(segs, "/")
}

func canonicalQuery(q url.Values) string {
	var pairs []string
	for k, vs := range q {
		for _, v := range vs {
			pairs = append(pairs, awsEscape(k)+"="+awsEscape(v))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

// awsEscape percent-encodes everything but SigV4's unreserved characters.
func awsEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z') || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}
