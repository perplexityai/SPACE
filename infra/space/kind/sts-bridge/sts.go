package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const stsNS = "https://sts.amazonaws.com/doc/2011-06-15/"

// forwarded are the AssumeRole parameters the bridge passes on unchanged.
// Anything else (ExternalId, Tags, PolicyArns, MFA, SourceIdentity) is
// refused rather than silently dropped.
var forwarded = map[string]bool{"RoleArn": true, "RoleSessionName": true, "Policy": true, "DurationSeconds": true}

type bridge struct {
	ids        map[string]identity
	signer     *signer
	issuer     string
	audience   string
	tokenTTL   time.Duration
	upstream   string
	httpClient *http.Client
	// now is the clock signatures are checked against; nil is time.Now.
	now func() time.Time
}

type upstreamResponse struct {
	Result struct {
		Credentials struct {
			AccessKeyID     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
			SessionToken    string `xml:"SessionToken"`
			Expiration      string `xml:"Expiration"`
		} `xml:"Credentials"`
		AssumedRoleUser struct {
			AssumedRoleID string `xml:"AssumedRoleId"`
			Arn           string `xml:"Arn"`
		} `xml:"AssumedRoleUser"`
		PackedPolicySize *int `xml:"PackedPolicySize"`
	} `xml:"AssumeRoleWithWebIdentityResult"`
}

type upstreamError struct {
	Error struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}

func (b *bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := newRequestID()
	start := time.Now()
	status, code, caller, role := b.handle(w, r, reqID)
	log.Printf("request_id=%s caller=%q role=%q status=%d code=%s elapsed_ms=%d", reqID, caller, role, status, code, time.Since(start).Milliseconds())
}

// handle answers one request and returns its status, STS code, verified
// caller subject and requested role, for the request log.
func (b *bridge) handle(w http.ResponseWriter, r *http.Request, reqID string) (int, string, string, string) {
	fail := func(e *authError) (int, string, string, string) {
		writeError(w, reqID, e)
		return e.Status, e.Code, "", ""
	}
	if r.Method != http.MethodPost || r.URL.Path != "/" {
		return fail(refuse(400, "InvalidAction", "only POST / is served"))
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		return fail(refuse(400, "MalformedInput", "could not read the body"))
	}
	clock := time.Now
	if b.now != nil {
		clock = b.now
	}
	id, aerr := verifySigV4(r, body, b.ids, clock())
	if aerr != nil {
		return fail(aerr)
	}
	if r.URL.RawQuery != "" {
		return fail(refuse(400, "InvalidParameterCombination", "parameters must be in the signed form body"))
	}
	params, err := url.ParseQuery(string(body))
	if err != nil {
		return fail(refuse(400, "MalformedInput", "body is not form-encoded"))
	}
	if params.Get("Action") != "AssumeRole" || params.Get("Version") != "2011-06-15" {
		return fail(refuse(400, "InvalidAction", "only AssumeRole, Version 2011-06-15, is served"))
	}
	up := url.Values{"Action": {"AssumeRoleWithWebIdentity"}, "Version": {"2011-06-15"}}
	for k, v := range params {
		if k == "Action" || k == "Version" {
			continue
		}
		if !forwarded[k] {
			return fail(refuse(400, "InvalidParameterValue", "parameter %s is not supported by this bridge", k))
		}
		up[k] = v
	}
	token, err := b.signer.mint(issuerClaims(b.issuer, id.Subject, b.audience, time.Now(), b.tokenTTL))
	if err != nil {
		return fail(refuse(500, "InternalFailure", "could not mint the identity token"))
	}
	up.Set("WebIdentityToken", token)

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.upstream, strings.NewReader(up.Encode()))
	if err != nil {
		e := refuse(500, "InternalFailure", "could not build the request to the store's STS")
		writeError(w, reqID, e)
		return e.Status, e.Code, id.Subject, up.Get("RoleArn")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := b.httpClient.Do(req)
	if err != nil {
		e := refuse(503, "ServiceUnavailable", "the store's STS is unreachable")
		writeError(w, reqID, e)
		return e.Status, e.Code, id.Subject, up.Get("RoleArn")
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		e := mapUpstreamError(resp.StatusCode, raw)
		writeError(w, reqID, e)
		return e.Status, e.Code, id.Subject, up.Get("RoleArn")
	}
	var out upstreamResponse
	if err := xml.Unmarshal(raw, &out); err != nil || out.Result.Credentials.AccessKeyID == "" {
		e := refuse(502, "InternalFailure", "the store's STS answered with no credentials")
		writeError(w, reqID, e)
		return e.Status, e.Code, id.Subject, up.Get("RoleArn")
	}
	writeAssumeRole(w, reqID, out)
	return 200, "-", id.Subject, up.Get("RoleArn")
}

// mapUpstreamError keeps the store's status and code, except where the code
// only makes sense for web identity: a refused token means the role does not
// trust this caller, which AssumeRole reports as AccessDenied.
func mapUpstreamError(status int, raw []byte) *authError {
	var ue upstreamError
	if xml.Unmarshal(raw, &ue) != nil || ue.Error.Code == "" {
		return refuse(502, "InternalFailure", "the store's STS answered %d", status)
	}
	switch ue.Error.Code {
	case "InvalidIdentityToken", "ExpiredTokenException":
		return refuse(403, "AccessDenied", "not authorized to perform sts:AssumeRole (store: %s: %s)", ue.Error.Code, ue.Error.Message)
	case "IDPCommunicationError":
		return refuse(503, "ServiceUnavailable", "the store could not reach the bridge's issuer (%s)", ue.Error.Message)
	}
	if status < 400 || status > 599 {
		status = 502
	}
	return refuse(status, ue.Error.Code, "%s", ue.Error.Message)
}

func writeAssumeRole(w http.ResponseWriter, reqID string, in upstreamResponse) {
	type creds struct {
		AccessKeyID     string `xml:"AccessKeyId"`
		SecretAccessKey string `xml:"SecretAccessKey"`
		SessionToken    string `xml:"SessionToken"`
		Expiration      string `xml:"Expiration"`
	}
	type result struct {
		Credentials     creds `xml:"Credentials"`
		AssumedRoleUser struct {
			AssumedRoleID string `xml:"AssumedRoleId"`
			Arn           string `xml:"Arn"`
		} `xml:"AssumedRoleUser"`
		PackedPolicySize *int `xml:"PackedPolicySize,omitempty"`
	}
	var out struct {
		XMLName  xml.Name `xml:"AssumeRoleResponse"`
		NS       string   `xml:"xmlns,attr"`
		Result   result   `xml:"AssumeRoleResult"`
		Metadata struct {
			RequestID string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	out.NS = stsNS
	c := in.Result.Credentials
	out.Result.Credentials = creds{c.AccessKeyID, c.SecretAccessKey, c.SessionToken, c.Expiration}
	out.Result.AssumedRoleUser.AssumedRoleID = in.Result.AssumedRoleUser.AssumedRoleID
	out.Result.AssumedRoleUser.Arn = in.Result.AssumedRoleUser.Arn
	out.Result.PackedPolicySize = in.Result.PackedPolicySize
	out.Metadata.RequestID = reqID
	writeXML(w, 200, reqID, out)
}

func writeError(w http.ResponseWriter, reqID string, e *authError) {
	kind := "Sender"
	if e.Status >= 500 {
		kind = "Receiver"
	}
	var out struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		NS      string   `xml:"xmlns,attr"`
		Error   struct {
			Type    string `xml:"Type"`
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
		RequestID string `xml:"RequestId"`
	}
	out.NS = stsNS
	out.Error.Type, out.Error.Code, out.Error.Message = kind, e.Code, e.Message
	out.RequestID = reqID
	writeXML(w, e.Status, reqID, out)
}

func writeXML(w http.ResponseWriter, status int, reqID string, v any) {
	body, err := xml.Marshal(v)
	if err != nil {
		http.Error(w, "xml", 500)
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	w.Header().Set("x-amzn-RequestId", reqID)
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "%s%s\n", xml.Header, body)
}

func newRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
