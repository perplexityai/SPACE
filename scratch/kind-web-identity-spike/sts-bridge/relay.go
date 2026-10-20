package main

// SPIKE (scratch/kind-web-identity-spike): web-identity base credentials for
// the credential-manager authority.
//
// The authority's default credential chain runs AssumeRoleWithWebIdentity
// with its projected ServiceAccount token, then signs AssumeRole for the vend
// role with the session that returns. Both calls go to AWS_ENDPOINT_URL_STS,
// this bridge. With -relay-web-identity the bridge:
//
//  1. relays AssumeRoleWithWebIdentity to versitygw's IAM service unchanged:
//     versitygw verifies the Kubernetes-issued token against the cluster's
//     JWKS (issuer, audience, expiry, pinned thumbprint) and the base role's
//     trust policy, and issues the session;
//  2. remembers the session it relayed (access key -> secret and token), the
//     only way it can later check a signature made with versitygw's secret;
//  3. on AssumeRole signed with such a session, verifies the signature, then
//     asks versitygw whether the session is still live (GetCallerIdentity
//     signed with it), and mints its token for the base role's ARN, which the
//     vend role's trust policy names. versitygw still enforces the vend
//     role's trust, maximum duration and session policy.

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// relayed are the AssumeRoleWithWebIdentity parameters passed on unchanged.
var relayed = map[string]bool{
	"Action": true, "Version": true, "RoleArn": true, "RoleSessionName": true,
	"WebIdentityToken": true, "DurationSeconds": true, "Policy": true,
}

const maxSessions = 4096

type sessionStore struct {
	mu sync.Mutex
	m  map[string]storedSession
}

type storedSession struct {
	id      identity
	expires time.Time
}

func newSessionStore() *sessionStore { return &sessionStore{m: map[string]storedSession{}} }

func (s *sessionStore) put(id identity, expires time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, v := range s.m {
		if now.After(v.expires) {
			delete(s.m, k)
		}
	}
	if len(s.m) >= maxSessions {
		for k := range s.m {
			delete(s.m, k)
			break
		}
	}
	s.m[id.AccessKey] = storedSession{id: id, expires: expires}
}

func (s *sessionStore) get(accessKey string) (identity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[accessKey]
	if !ok || time.Now().After(v.expires) {
		return identity{}, false
	}
	return v.id, true
}

// callerIDs is the static identities plus the relayed session named by the
// request's credential, if any.
func (b *bridge) callerIDs(r *http.Request) map[string]identity {
	if b.sessions == nil {
		return b.ids
	}
	_, cred, _ := strings.Cut(r.Header.Get("Authorization"), "Credential=")
	akid, _, _ := strings.Cut(cred, "/")
	sess, ok := b.sessions.get(akid)
	if !ok {
		return b.ids
	}
	ids := make(map[string]identity, len(b.ids)+1)
	for k, v := range b.ids {
		ids[k] = v
	}
	ids[akid] = sess
	return ids
}

// relayWebIdentity forwards an unsigned AssumeRoleWithWebIdentity to the
// store's IAM service and answers with its response, keeping the session.
func (b *bridge) relayWebIdentity(w http.ResponseWriter, r *http.Request, params url.Values, reqID string) (int, string, string, string) {
	role := params.Get("RoleArn")
	for k := range params {
		if !relayed[k] {
			e := refuse(400, "InvalidParameterValue", "parameter %s is not supported by this bridge", k)
			writeError(w, reqID, e)
			return e.Status, e.Code, "", role
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	resp, raw, err := b.postUpstream(ctx, params, nil)
	if err != nil {
		e := refuse(503, "ServiceUnavailable", "the store's STS is unreachable")
		writeError(w, reqID, e)
		return e.Status, e.Code, "web-identity", role
	}
	if resp.StatusCode != http.StatusOK {
		// The store's code passes through: IDPCommunicationError,
		// InvalidIdentityToken and AccessDenied mean to the SDK what they mean
		// from AWS STS.
		var ue upstreamError
		if xml.Unmarshal(raw, &ue) != nil || ue.Error.Code == "" {
			e := refuse(502, "InternalFailure", "the store's STS answered %d", resp.StatusCode)
			writeError(w, reqID, e)
			return e.Status, e.Code, "web-identity", role
		}
		e := refuse(resp.StatusCode, ue.Error.Code, "%s", ue.Error.Message)
		writeError(w, reqID, e)
		return e.Status, e.Code, "web-identity", role
	}
	var out upstreamResponse
	if err := xml.Unmarshal(raw, &out); err != nil || out.Result.Credentials.AccessKeyID == "" {
		e := refuse(502, "InternalFailure", "the store's STS answered with no credentials")
		writeError(w, reqID, e)
		return e.Status, e.Code, "web-identity", role
	}
	c := out.Result.Credentials
	expires, err := time.Parse(time.RFC3339, c.Expiration)
	if err != nil {
		expires = time.Now().Add(15 * time.Minute)
	}
	b.sessions.put(identity{AccessKey: c.AccessKeyID, Secret: c.SecretAccessKey, SessionToken: c.SessionToken,
		Subject: roleOfAssumed(out.Result.AssumedRoleUser.Arn)}, expires)
	w.Header().Set("Content-Type", "text/xml")
	w.Header().Set("x-amzn-RequestId", reqID)
	w.WriteHeader(200)
	_, _ = w.Write(raw)
	return 200, "-", "web-identity:" + roleOfAssumed(out.Result.AssumedRoleUser.Arn), role
}

// confirmSession asks the store whether a relayed session is still live and
// still the role it was issued for; the answer, not the bridge's memory, is
// what the vend token is minted for.
func (b *bridge) confirmSession(ctx context.Context, id identity) (string, *authError) {
	params := url.Values{"Action": {"GetCallerIdentity"}, "Version": {"2011-06-15"}}
	resp, raw, err := b.postUpstream(ctx, params, &id)
	if err != nil {
		return "", refuse(503, "ServiceUnavailable", "the store's STS is unreachable")
	}
	if resp.StatusCode != http.StatusOK {
		var ue upstreamError
		_ = xml.Unmarshal(raw, &ue)
		if resp.StatusCode >= 500 {
			return "", refuse(503, "ServiceUnavailable", "the store's STS answered %d", resp.StatusCode)
		}
		return "", refuse(403, "InvalidClientTokenId", "the store refused the session (%s)", ue.Error.Code)
	}
	var out struct {
		Result struct {
			Arn string `xml:"Arn"`
		} `xml:"GetCallerIdentityResult"`
	}
	if xml.Unmarshal(raw, &out) != nil || out.Result.Arn == "" {
		return "", refuse(502, "InternalFailure", "the store's GetCallerIdentity answered with no ARN")
	}
	role := roleOfAssumed(out.Result.Arn)
	if role != id.Subject {
		return "", refuse(403, "InvalidClientTokenId", "the session's role changed")
	}
	return role, nil
}

// postUpstream posts a form to the store's IAM service, SigV4-signed with
// signAs when set.
func (b *bridge) postUpstream(ctx context.Context, params url.Values, signAs *identity) (*http.Response, []byte, error) {
	body := params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.upstream, strings.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if signAs != nil {
		signSTS(req, []byte(body), *signAs, b.region, time.Now())
	}
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp, raw, nil
}

// signSTS signs req for service sts with the canonicalization verifySigV4
// checks.
func signSTS(req *http.Request, body []byte, id identity, region string, now time.Time) {
	amzDate := now.UTC().Format(amzDateForm)
	req.Header.Set("X-Amz-Date", amzDate)
	if id.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", id.SessionToken)
	}
	names := []string{"content-type", "host", "x-amz-date"}
	if id.SessionToken != "" {
		names = append(names, "x-amz-security-token")
	}
	req.Host = req.URL.Host
	var canon strings.Builder
	canon.WriteString(req.Method + "\n" + canonicalPath(req.URL.EscapedPath()) + "\n" + canonicalQuery(req.URL.Query()) + "\n")
	for _, n := range names {
		v := req.Host
		if n != "host" {
			v = strings.Join(trimAll(req.Header.Values(n)), ",")
		}
		canon.WriteString(n + ":" + v + "\n")
	}
	signed := strings.Join(names, ";")
	canon.WriteString("\n" + signed + "\n" + sha256Hex(body))
	scope := amzDate[:8] + "/" + region + "/sts/aws4_request"
	toSign := sigAlgorithm + "\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canon.String()))
	key := hmacSHA256([]byte("AWS4"+id.Secret), amzDate[:8])
	for _, s := range []string{region, "sts", "aws4_request"} {
		key = hmacSHA256(key, s)
	}
	sig := fmt.Sprintf("%x", hmacSHA256(key, toSign))
	req.Header.Set("Authorization", fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		sigAlgorithm, id.AccessKey, scope, signed, sig))
}

// roleOfAssumed maps arn:aws:sts::<acct>:assumed-role/<role>/<session> to
// arn:aws:iam::<acct>:role/<role>, the principal a trust policy names.
func roleOfAssumed(arn string) string {
	rest, ok := strings.CutPrefix(arn, "arn:aws:sts::")
	if !ok {
		return arn
	}
	acct, path, _ := strings.Cut(rest, ":")
	parts := strings.Split(strings.TrimPrefix(path, "assumed-role/"), "/")
	return "arn:aws:iam::" + acct + ":role/" + parts[0]
}
