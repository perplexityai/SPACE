# Spike: web-identity credentials for the credential authority and cluster-controller on Kind

Base: `origin/main` 188c1a3. Prototype: this directory. Nothing outside `scratch/` changes.

## Verdicts

| Workload | Verdict | Recommended design |
|---|---|---|
| cluster-controller | **GO** | EKS gives it **Pod Identity** (`aws_eks_pod_identity_association` in `db_client_pod_identity.tf`), not IRSA. The STS bridge serves as the Pod Identity agent: the SDK's container provider sends the projected token, and the bridge exchanges that token at versitygw for the controller's role. Plain web identity straight to versitygw also works, and needs no bridge code. |
| credential authority | **GO WITH CONDITIONS** | IRSA as on EKS. The bridge relays the authority's `AssumeRoleWithWebIdentity` to versitygw unchanged and keeps the session it returns. It then verifies the authority's session-signed `AssumeRole`, has versitygw confirm that session (`GetCallerIdentity`), and exchanges for the vend role with a token that names the base role. The authority's code is unchanged. The conditions are below. |

Conditions for the authority:
1. The bridge keeps relayed session secrets in memory. It must therefore stay at one replica, and an exchange that straddles a bridge restart fails (`InvalidClientTokenId`, then `FAILED_PRECONDITION`). A stateless follow-up would derive the secret it returns from the access key and seal versitygw's real secret into the session token.
2. An issuer outage surfaces as `FAILED_PRECONDITION` even with #642. versitygw answers a failed JWKS fetch the way AWS can: `InvalidIdentityToken` "Couldn't retrieve verification key from your identity provider". #642 treats that code as permanent. The bench would show this honestly. Whether that row should be `UNAVAILABLE` is a question for #642, not for Kind.
3. Kind needs a cluster-scoped binding that opens issuer discovery to anonymous callers, and it must pin the apiserver's serving leaf on every bring-up.

## Q1: kube-apiserver as the OIDC issuer: yes

- **Issuer:** `https://kubernetes.default.svc.cluster.local` (kind's default), with `jwks_uri` `https://<control-plane IP>:6443/openid/v1/jwks`.
- **Reachability:** `system:service-account-issuer-discovery` is bound only to `system:serviceaccounts`, so versitygw's anonymous fetches got 403. `manifests/oidc-discovery-rbac.yaml` binds it to `system:unauthenticated` (public keys only, as Kubernetes documents), and both URLs then return 200.
- **Private address:** versitygw refuses private targets unless `--oidc-allow-private-endpoints` (global) is set, or an `--oidc-discovery-url` mapping covers the issuer (`OIDCEndpointPolicy.ResolveDiscovery`, one issuer only, JWKS fetch included). The prototype adds `https://kubernetes.default.svc.cluster.local=https://kubernetes.default.svc/.well-known/openid-configuration` to the IAM Deployment.
- **Thumbprint:** the apiserver serves only its leaf (`CN=kube-apiserver`). versitygw pins the top certificate served, which is that leaf, and verifies the leaf against itself (`verifyOIDCConnection`). `spike.sh` reads its SHA-1; every new cluster needs a new pin.
- **Audience:** `sts.amazonaws.com` (IRSA) and `pods.eks.amazonaws.com` (Pod Identity), as provider client IDs and trust conditions.
- **Evidence:** a real exchange from the probe pod's projected token:

```
$ aws sts assume-role-with-web-identity --endpoint-url https://object-store-iam:7070 \
    --role-arn arn:aws:iam::000000000000:role/kind-spike-probe --web-identity-token "$(cat .../token)"
ASIAAFEZ…  2026-10-20T06:16:35+00:00  arn:aws:sts::000000000000:assumed-role/kind-spike-probe/q1
role trusting another SA  -> InvalidIdentityToken
tampered signature        -> InvalidIdentityToken
token with aud other      -> InvalidIdentityToken
S3 ListBucket with the session (role has no policy) -> AccessDenied … no identity-based policy allows s3:ListBucket
```

## Q2: cluster-controller: yes, with no static keys

- **Needs:** the snapshot storage and retention sweepers call `S3StorageBackend` (`ListObjectsV2` and `DeleteObjects` on the URI's bucket) through the SDK default chain (`aws_config::load_defaults` in `reconcilers.rs`). The role policy is the EKS module's `cluster_controller_snapshots_reap`: `s3:ListBucket` on `snapshots` and `s3:DeleteObject` on `snapshots/*`.
- **Web identity:** `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE` (audience `sts.amazonaws.com`), `AWS_ENDPOINT_URL_STS=https://object-store-iam:7070` and `SSL_CERT_DIR` (CA already mounted), with the controller in the IAM STS ingress rule.
- **Pod Identity:** `AWS_CONTAINER_CREDENTIALS_FULL_URI=https://sts-bridge:8080/v1/credentials` and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` (audience `pods.eks.amazonaws.com`), as the EKS webhook injects them. The bridge maps the ServiceAccount to the role (`-pod-identity-roles`) and exchanges the token at versitygw (`podidentity.go`).
- **Evidence:** in both modes the controller's pod held no `AWS_ACCESS_KEY_ID`. With web identity, in an ephemeral aws-cli container in the controller pod using its own projected token:

```
get-caller-identity  -> arn:aws:sts::000000000000:assumed-role/kind-cluster-controller/…
list snapshots       -> 2 True          delete-objects snapshots -> {"Deleted":[…]}
put snapshots / get snapshots object / list templates -> AccessDenied
```

  `local.retention_claim_finish_forward` passed in both modes. In each, the controller pod that logged `snapshot_retention_sweeper: cleared storage prefix` had only the identity env. In Pod Identity mode the bridge logged `caller="pod-identity:system:serviceaccount:space:space-cluster-controller" … status=200`.

## Q3: the authority: base credentials from web identity, then the vend `AssumeRole`

Alternatives checked:
- **versitygw accepts `AssumeRole`: no.** v1.9.0 and main 007a8589 serve only `AssumeRoleWithWebIdentity` and `GetCallerIdentity` (`stsActions`, `iamapi/router.go`). A session-signed `AssumeRole` sent there gets `SignatureDoesNotMatch: Credential should be scoped to correct service: 'iam'`, so relaying the authority's signed request verbatim checks nothing either.
- **The bridge verifies the projected token and mints its own sessions: rejected.** The bridge would then enforce the base identity, not versitygw.
- **Relay and confirm: chosen** (`sts-bridge/relay.go`, flag `-relay-web-identity`). Both of the SDK's calls go to `AWS_ENDPOINT_URL_STS`; aws-config's `ProviderConfig::client_config` threads the env service config into the web-identity provider's STS client. Then:
  1. The unsigned `AssumeRoleWithWebIdentity` is relayed unchanged. versitygw verifies the token, issuer, audience and expiry, and the base role's trust. The trust names `system:serviceaccount:space:space-credmgr-authority`, and the role has no policy, as `credential_manager_iam` attaches none. The bridge keeps `{access key → secret, token}`.
  2. The session-signed `AssumeRole` is verified with that secret, and must carry the signed `X-Amz-Security-Token`.
  3. `GetCallerIdentity`, signed with the session, makes versitygw confirm the session is live and its role unchanged.
  4. The bridge mints its token with `sub` = the base role's ARN, and the vend role's trust names that ARN. versitygw still enforces the trust, maximum duration, session policy and expiry.
- **Evidence:** `local.storage_exchange_scoping` passed with the authority on web identity. The bridge log shows one relay and one session-signed `AssumeRole` per exchange (10 and 10):

```
caller="web-identity:arn:aws:iam::000000000000:role/kind-credmgr-authority" role=…/kind-credmgr-authority status=200
caller="arn:aws:iam::000000000000:role/kind-credmgr-authority" role=…/kind-storage-vend status=200
probe SA's base session -> vend AssumeRole: AccessDenied (store: InvalidIdentityToken)   # versitygw refused the trust
same, forged session token:                 InvalidClientTokenId                          # bridge, nothing sent upstream
```

## Q4: base-identity outages, with and without #642

#642 (`d4f4801`, open) was cherry-picked onto 188c1a3 cleanly. Each variant is a single-package build by one command (`build-credmgr-variants.sh`), sha256 `bbd2866d…` (main) and `6de00ba1…` (#642), checked inside both running authority pods. The authority was on web identity throughout. `q4-matrix.sh` sends one `ExchangeToken` per row on a worker agent's volume socket, as volume-manager does, and prints the code and the `x-space-credmgr-error-reason` metadata.

| Outage | main | main + #642 |
|---|---|---|
| none | OK | OK |
| bridge's web-identity endpoint (bridge scaled to 0) | `FAILED_PRECONDITION` | `UNAVAILABLE`, `sts-unavailable` |
| IAM service (scaled to 0; the bridge answers 503) | `FAILED_PRECONDITION` | `UNAVAILABLE`, `sts-unavailable` |
| issuer (anonymous discovery binding removed, IAM restarted to drop its 5-minute JWKS cache) | `FAILED_PRECONDITION` | `FAILED_PRECONDITION` (versitygw: `InvalidIdentityToken`, "Couldn't retrieve verification key…") |
| each source restored | OK | OK |

## Q5: cost

- **Components:** no new Deployment. The bridge gains about 300 lines of Go, the IAM Deployment one discovery mapping, and bucket-init one provider and two roles.
- **TLS:** nothing new. The controller gains `SSL_CERT_DIR` for the internal RPC CA, and the apiserver link is pinned by thumbprint.
- **NetworkPolicy:** with Pod Identity the controller reaches only the bridge; plain web identity must join the IAM STS rule. The bridge's STS port has no ingress policy today, and should admit only the authority and the controller.
- **RBAC:** one ClusterRoleBinding (anonymous issuer discovery).
- **Bring-up:** `spike.sh apply` took 26–39 s live, mostly the IAM `Recreate` restart. Inside bucket-init it would add seconds.
- **STS calls per exchange:**
  - The authority reloads its SDK config per exchange (`assume_s3_vend_role`), so its calls to the bridge go from 1 to 2.
  - The bridge's calls to IAM go from 1 to 3: the relay, `GetCallerIdentity` and the vend exchange.
  - IAM's discovery and JWKS fetches are cached for 5 minutes, with a forced refresh for an unknown kid at most every 30 s.
  - Bridge time per exchange went from a median of 8 ms (n=10) to 5.5 ms + 11 ms (n=20).
- **Failure modes it adds:**
  - Issuer reachability: the RBAC binding, and the apiserver leaf pin, which goes stale if the leaf changes.
  - The bridge's in-memory sessions (condition 1).
  - For a few seconds after an IAM restart, connections are refused (observed), which surfaces as `UNAVAILABLE` with #642.
  - An issuer outage reads as permanent (condition 2).
  - Tilt's `prune-superseded-images` removes unreferenced `space-*` images from the nodes, so ad hoc variant images need another repository name.

## Prototype and its shortcuts

- `sts-bridge/`: a copy of `infra/space/kind/sts-bridge` plus `relay.go`, `podidentity.go`, a `relay_test.go` round trip and session-token checks in `sigv4.go`. `Dockerfile.sts-bridge` builds it.
- `spike.sh apply|revert|status`: patches the live IAM args and NetworkPolicy, the bridge image and flags, and the authority's and controller's env and tokens (`SPIKE_CONTROLLER_IDENTITY=pod-identity` for the controller). It runs `manifests/iam-setup-job.yaml` and the RBAC. `revert` restores the saved objects and runs `iam-cleanup-job.yaml`.
- Q4: `q4-fixture.sh`, `q4_exchange.py`, `q4-matrix.sh` and `build-credmgr-variants.sh`.

Shortcuts:
- Live patches instead of chart changes; a Tilt re-apply reverts them.
- The setup and cleanup Jobs and the probe pod borrow other workloads' labels to pass the IAM NetworkPolicy.
- The vend trust lists both the static subject and the base role.
- `BRIDGE_IDENTITIES` (the static vend keys) stays configured.
- Sessions live in an unbounded-age map capped at 4096 entries.
- Pod Identity reads the token's ServiceAccount unverified, only to choose the role.
- No bridge NetworkPolicy.
- The Q4 binaries are single-package builds.

## A follow-up PR would

1. **Bridge** (`infra/space/kind/sts-bridge`): add the relay, the session-signed `AssumeRole` and the Pod Identity endpoint with Go behavioural tests at that layer. Use stateless sealed session credentials, or document the one-replica limit. Drop `BRIDGE_IDENTITIES` and the vend keys in `sts-bridge-secret.sh`. Add a NetworkPolicy for the bridge's STS port.
2. **`space-object-store`:**
   - In bucket-init, register the cluster issuer (client IDs `sts.amazonaws.com` and `pods.eks.amazonaws.com`) with a leaf thumbprint that a script computes per bring-up, carried as a checksum annotation as `sts-bridge-secret.sh` does.
   - Add roles `kind-credmgr-authority` (no policy) and `kind-cluster-controller` (the snapshots reap policy).
   - Set the vend trust `sub` to the authority role ARN only.
   - Add the IAM `--oidc-discovery-url` mapping.
3. **`manifests/`:** add the anonymous issuer-discovery ClusterRoleBinding, applied in both bring-up paths.
4. **`space-credmgr-authority-local`:** in exchange mode, replace the bridge's static keys with `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE` and a projected token (audience `sts.amazonaws.com`). `static` mode is unchanged.
5. **`space-cluster-controller-local`:** replace the store's root keys with the Pod Identity env, a projected token and `SSL_CERT_DIR`.
6. **README "Differences" rows:** the storage-exchange row (*"the authority signs `AssumeRole` with the bridge's static vend keys…"*) and the cluster-controller row (*"the store's static root keys … Kind has no workload identity provider to reproduce"*).
7. **Validation:** `local.storage_exchange_scoping`, the retention scenarios and `smoke-kind-e2e.sh`. Once #642 merges, add a typed scenario that takes out the bridge or IAM and expects `UNAVAILABLE`/`sts-unavailable`, alongside #478's store-outage scenarios.

## Validation

A fresh Kind cluster from 188c1a3, with CI's settings (`SPACE_KIND_TELEMETRY_MODE=otel`, `DATA_PLANE_URL`, admin token file and cold-receipts directory). The health gate ran before each suite. 05ac1c9 is the prototype as validated; the pushed commit differs only in this report.

| Run | Tree (base 188c1a3) | Identity | Result |
|---|---|---|---|
| `smoke-kind-e2e.sh`, cold bring-up | main | stock | PASS |
| `smoke-kind-e2e.sh` (`SKIP_BOOTSTRAP=1`) | 05ac1c9 | authority on IRSA, controller on Pod Identity | PASS |
| `local.storage_exchange_scoping` | 05ac1c9 | same | PASS |
| `local.suspend_restore_cross_node_from_object_storage` | 05ac1c9 | same | PASS |
| `local.retention_claim_finish_forward` | 05ac1c9 | same | PASS; prefix cleared by a controller pod with only Pod Identity env |
| Q4 matrix | 05ac1c9, authority variants | authority on IRSA | as in Q4 |
| the same suites, earlier iteration | before `podidentity.go` | both on web identity | PASS |
| `spike.sh revert`, then `smoke-kind-e2e.sh` | main | stock | PASS |
| `gofmt`, `go vet`, `go test` (in `Dockerfile.sts-bridge`) | 05ac1c9 | — | PASS |

The bridge served 38 relays and 38 session-signed `AssumeRole` calls in the 05ac1c9 suites, all 200.

Provenance: the bridge pod ran node image `af28c2cd…` (binary `28199ea1…`), built by `spike.sh` from the clean 05ac1c9 tree. The authority and controller ran Tilt's main build (authority binary `19c74573…`). No Rust changed, so no cargo checks.
