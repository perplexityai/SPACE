// Command space-kind-sts-bridge is the Kind-only STS front for the object
// store (infra/space/kind/charts/space-kind-sts-bridge). The
// credential-manager authority calls AWS STS AssumeRole, unchanged, to vend
// storage sessions; versitygw's IAM service serves only
// AssumeRoleWithWebIdentity. The bridge verifies the AssumeRole's SigV4
// signature against the vend identities it holds, mints a short-lived OIDC
// token for the verified identity, exchanges it at versitygw with the same
// RoleArn, RoleSessionName, Policy and DurationSeconds, and answers in
// AssumeRole's format. versitygw enforces the role's trust policy, its
// maximum session duration, the session policy and expiry. The bridge also
// serves the token issuer's discovery document and JWKS, which versitygw
// fetches over HTTPS.
//
// It is a test fixture, like space-kind-key-broker: it stands in for the
// workload identity and AWS STS a deployment uses, and is never deployed
// outside a local cluster.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("space-kind-sts-bridge", flag.ExitOnError)
	listen := fs.String("listen", ":8080", "STS listen address (HTTPS), the authority's AWS_ENDPOINT_URL_STS")
	stsCert := fs.String("sts-tls-cert", "", "STS listener certificate chain, leaf first")
	stsKey := fs.String("sts-tls-key", "", "STS listener key")
	issuerListen := fs.String("issuer-listen", ":8443", "issuer listen address (HTTPS)")
	issuerCert := fs.String("issuer-tls-cert", "", "issuer certificate chain, leaf first, ending with the CA versitygw pins")
	issuerKey := fs.String("issuer-tls-key", "", "issuer listener key")
	issuer := fs.String("issuer", "", "iss claim, as registered with versitygw")
	jwksBase := fs.String("jwks-base", "", "base URL of jwks_uri in the discovery document (default: -issuer)")
	keyPath := fs.String("signing-key", "", "PKCS#8 RSA token signing key")
	audience := fs.String("audience", "", "aud claim, a client ID of versitygw's OIDC provider")
	upstream := fs.String("upstream", "", "versitygw IAM service URL (https)")
	upstreamCA := fs.String("upstream-ca", "", "CA bundle the upstream's certificate must chain to")
	ttl := fs.Duration("token-ttl", 5*time.Minute, "minted token lifetime")
	_ = fs.Parse(args)

	for _, f := range []struct{ name, value string }{
		{"-sts-tls-cert", *stsCert}, {"-sts-tls-key", *stsKey}, {"-issuer-tls-cert", *issuerCert},
		{"-issuer-tls-key", *issuerKey}, {"-issuer", *issuer}, {"-signing-key", *keyPath},
		{"-audience", *audience}, {"-upstream", *upstream}, {"-upstream-ca", *upstreamCA},
	} {
		if f.value == "" {
			return fmt.Errorf("%s is required", f.name)
		}
	}
	ids, err := parseIdentities(os.Getenv("BRIDGE_IDENTITIES"))
	if err != nil {
		return err
	}
	s, err := loadSigner(*keyPath)
	if err != nil {
		return err
	}
	client, err := upstreamClient(*upstream, *upstreamCA)
	if err != nil {
		return err
	}
	if *jwksBase == "" {
		*jwksBase = *issuer
	}
	b := &bridge{ids: ids, signer: s, issuer: *issuer, audience: *audience, tokenTTL: *ttl, upstream: *upstream,
		httpClient: client}
	errs := make(chan error, 2)
	go func() {
		errs <- serveTLS(*issuerListen, *issuerCert, *issuerKey, issuerHandler(*issuer, *jwksBase, s), defaultLimits)
	}()
	go func() { errs <- serveTLS(*listen, *stsCert, *stsKey, b, defaultLimits) }()
	log.Printf("sts on %s, issuer %s on %s (kid %s), %d identities, upstream %s",
		*listen, *issuer, *issuerListen, s.kid, len(ids), *upstream)
	return <-errs
}

// parseIdentities reads "ACCESS_KEY:SECRET:SUBJECT[,...]" from the
// environment, so secrets never appear in the process's argv.
func parseIdentities(spec string) (map[string]identity, error) {
	ids := map[string]identity{}
	for _, entry := range strings.Split(spec, ",") {
		if entry = strings.TrimSpace(entry); entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ":", 3)
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return nil, fmt.Errorf("BRIDGE_IDENTITIES entry must be ACCESS_KEY:SECRET:SUBJECT")
		}
		ids[parts[0]] = identity{AccessKey: parts[0], Secret: parts[1], Subject: parts[2]}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("BRIDGE_IDENTITIES is empty")
	}
	return ids, nil
}
