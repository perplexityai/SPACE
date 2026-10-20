package main

// SPIKE (scratch/kind-web-identity-spike): an EKS Pod Identity agent stand-in.
//
// On EKS, cluster-controller's credentials come from Pod Identity: the
// webhook sets AWS_CONTAINER_CREDENTIALS_FULL_URI and
// AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE, and the SDK's container provider
// GETs the agent with the projected token (audience pods.eks.amazonaws.com)
// as its Authorization header. Here the bridge answers that GET by
// exchanging the same token at versitygw (AssumeRoleWithWebIdentity) for the
// role associated with the token's ServiceAccount; versitygw verifies the
// token and the role's trust. The ServiceAccount is read from the token
// unverified, only to pick the role: a forged claim buys nothing versitygw's
// check would not refuse.

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// parsePodRoles reads "namespace/serviceaccount=roleArn[,...]".
func parsePodRoles(spec string) map[string]string {
	roles := map[string]string{}
	for _, entry := range strings.Split(spec, ",") {
		if sa, role, ok := strings.Cut(strings.TrimSpace(entry), "="); ok {
			ns, name, _ := strings.Cut(sa, "/")
			roles["system:serviceaccount:"+ns+":"+name] = role
		}
	}
	return roles
}

func (b *bridge) podIdentity(w http.ResponseWriter, r *http.Request) (int, string, string, string) {
	fail := func(status int, code, msg, sub, role string) (int, string, string, string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": msg})
		return status, code, sub, role
	}
	token := strings.TrimSpace(r.Header.Get("Authorization"))
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return fail(400, "InvalidTokenException", "missing or malformed token", "", "")
	}
	payload, err := b64.DecodeString(parts[1])
	var claims struct {
		Sub string `json:"sub"`
		K8s struct {
			Pod struct {
				Name string `json:"name"`
			} `json:"pod"`
		} `json:"kubernetes.io"`
	}
	if err != nil || json.Unmarshal(payload, &claims) != nil {
		return fail(400, "InvalidTokenException", "malformed token", "", "")
	}
	role, ok := b.podRoles[claims.Sub]
	if !ok {
		return fail(400, "AccessDeniedException", "no role is associated with "+claims.Sub, claims.Sub, "")
	}
	session := "eks-pod-identity-" + claims.K8s.Pod.Name
	if len(session) > 64 {
		session = session[:64]
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	params := url.Values{"Action": {"AssumeRoleWithWebIdentity"}, "Version": {"2011-06-15"},
		"RoleArn": {role}, "RoleSessionName": {session}, "WebIdentityToken": {token}}
	resp, raw, err := b.postUpstream(ctx, params, nil)
	if err != nil {
		return fail(503, "ServiceUnavailableException", "the store's STS is unreachable", claims.Sub, role)
	}
	if resp.StatusCode != http.StatusOK {
		var ue upstreamError
		_ = xml.Unmarshal(raw, &ue)
		status := 400
		if resp.StatusCode >= 500 {
			status = 500
		}
		return fail(status, ue.Error.Code, ue.Error.Message, claims.Sub, role)
	}
	var out upstreamResponse
	if err := xml.Unmarshal(raw, &out); err != nil || out.Result.Credentials.AccessKeyID == "" {
		return fail(500, "InternalFailure", "the store's STS answered with no credentials", claims.Sub, role)
	}
	c := out.Result.Credentials
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"AccessKeyId": c.AccessKeyID, "SecretAccessKey": c.SecretAccessKey, "Token": c.SessionToken,
		"Expiration": c.Expiration, "AccountId": "000000000000",
	})
	return 200, "-", "pod-identity:" + claims.Sub, role
}
