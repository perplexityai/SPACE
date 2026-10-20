package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// relayIAM is a fake store STS: it issues one session for a web identity
// token and answers GetCallerIdentity only when signed with that session.
type relayIAM struct{ calls []string }

func (f *relayIAM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	form, _ := url.ParseQuery(string(body))
	f.calls = append(f.calls, form.Get("Action"))
	switch form.Get("Action") {
	case "AssumeRoleWithWebIdentity":
		exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>ASIABASE</AccessKeyId><SecretAccessKey>base-secret</SecretAccessKey><SessionToken>base-token</SessionToken><Expiration>%s</Expiration></Credentials><AssumedRoleUser><Arn>arn:aws:sts::000000000000:assumed-role/kind-authority/s</Arn></AssumedRoleUser></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`, exp)
	case "GetCallerIdentity":
		r.Body = io.NopCloser(strings.NewReader(string(body)))
		ids := map[string]identity{"ASIABASE": {AccessKey: "ASIABASE", Secret: "base-secret", SessionToken: "base-token"}}
		if _, err := verifySigV4(r, body, ids, time.Now()); err != nil {
			w.WriteHeader(403)
			fmt.Fprintf(w, `<ErrorResponse><Error><Code>%s</Code></Error></ErrorResponse>`, err.Code)
			return
		}
		fmt.Fprint(w, `<GetCallerIdentityResponse><GetCallerIdentityResult><Arn>arn:aws:sts::000000000000:assumed-role/kind-authority/s</Arn></GetCallerIdentityResult></GetCallerIdentityResponse>`)
	default:
		_, _ = io.WriteString(w, webIdentityOK)
	}
}

func TestRelayedSessionSignsAssumeRole(t *testing.T) {
	iam := &relayIAM{}
	upstream := httptest.NewServer(iam)
	defer upstream.Close()
	b := &bridge{ids: map[string]identity{}, signer: testSigner(t), issuer: testIssuer, audience: testAudience,
		tokenTTL: time.Minute, upstream: upstream.URL, httpClient: upstream.Client(), sessions: newSessionStore(), region: "us-east-1"}

	form := url.Values{"Action": {"AssumeRoleWithWebIdentity"}, "Version": {"2011-06-15"},
		"RoleArn": {"arn:aws:iam::000000000000:role/kind-authority"}, "RoleSessionName": {"s"}, "WebIdentityToken": {"k8s"}}
	r := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	rec := httptest.NewRecorder()
	b.ServeHTTP(rec, r)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "ASIABASE") {
		t.Fatalf("relay: %d %s", rec.Code, rec.Body)
	}

	body := "Action=AssumeRole&Version=2011-06-15&RoleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Fkind-storage-vend&RoleSessionName=x"
	r = httptest.NewRequest("POST", "https://sts-bridge:8080/", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	signSTS(r, []byte(body), identity{AccessKey: "ASIABASE", Secret: "base-secret", SessionToken: "base-token"}, "us-east-1", time.Now())
	rec = httptest.NewRecorder()
	b.ServeHTTP(rec, r)
	if rec.Code != 200 {
		t.Fatalf("assume role: %d %s", rec.Code, rec.Body)
	}
	if strings.Join(iam.calls, ",") != "AssumeRoleWithWebIdentity,GetCallerIdentity,AssumeRoleWithWebIdentity" {
		t.Fatalf("upstream calls %v", iam.calls)
	}

	// The wrong token is refused before anything reaches the store.
	r = httptest.NewRequest("POST", "https://sts-bridge:8080/", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	signSTS(r, []byte(body), identity{AccessKey: "ASIABASE", Secret: "base-secret", SessionToken: "other"}, "us-east-1", time.Now())
	rec = httptest.NewRecorder()
	b.ServeHTTP(rec, r)
	if rec.Code != 403 || len(iam.calls) != 3 {
		t.Fatalf("forged token: %d %s, calls %v", rec.Code, rec.Body, iam.calls)
	}
}
