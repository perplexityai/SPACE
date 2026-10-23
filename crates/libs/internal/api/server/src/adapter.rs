use pplx_space_common_core::types::OrgId;
use pplx_space_common_svc::template_lookup::TemplateLookupScope;

use super::metadata::*;
use super::status::*;
use super::*;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    BaseSnapshotShape, TemplateDesiredState as DomainTemplateDesiredState,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TemplateOrg {
    System,
    Owned { org_id: OrgId },
}

impl TemplateOrg {
    fn parse(org_id: Option<&str>) -> Result<TemplateOrg, Status> {
        match org_id {
            None => Ok(TemplateOrg::System),
            Some(org_id) => {
                let org_id = OrgId::from_uuid(parse_uuid_field(org_id, "org_id")?);
                Ok(TemplateOrg::Owned { org_id })
            }
        }
    }

    fn org_id(self) -> Option<OrgId> {
        match self {
            TemplateOrg::System => None,
            TemplateOrg::Owned { org_id } => Some(org_id),
        }
    }
}

fn parse_uuid_field(value: &str, field: &'static str) -> Result<Uuid, Status> {
    Uuid::parse_str(value.trim())
        .map_err(|error| Status::invalid_argument(format!("{field} must be a UUID: {error}")))
}

fn project_sandbox_network_policy_states(
    rows: Vec<SandboxPolicyRow>,
) -> Vec<SandboxNetworkPolicyState> {
    let mut states = Vec::with_capacity(rows.len());
    for row in rows {
        // The node fences the sandbox's apply on the sandbox's own generation
        // lineage, whether the effective policy is an override or inherited.
        // Using the template generation on the inherited path wedged a clear
        // (see `sandbox_effective_policy_generation`).
        let generation = super::metadata::sandbox_effective_policy_generation(&row);
        let own_policy = row.network_policy_json.map(|policy| policy.0);
        let template_policy = row
            .template_network_policy_json
            .map(|policy| policy.0)
            .unwrap_or_else(NetworkPolicyV1::deny_all);
        let inherited = own_policy.is_none();
        let effective_policy = own_policy.unwrap_or(template_policy);
        states.push(SandboxNetworkPolicyState {
            sandbox_id: row.sandbox_id.to_string(),
            source: if inherited {
                NetworkPolicySource::Template as i32
            } else {
                NetworkPolicySource::Override as i32
            },
            policy: Some(common_network_policy_to_proto(&effective_policy)),
            generation: db_generation_to_proto(generation),
            template: row.template_id.unwrap_or_default(),
        });
    }
    states
}

impl ClusterApiService {
    /// Derive the template-catalog scope from the authenticated caller.
    ///
    /// `caller_org_id` on the lookup requests is caller-asserted and the node
    /// pull path omits it, so it cannot authorize anything. A node-scoped
    /// caller (every TokenReview identity, and the static token once
    /// TokenReview is configured) is instead bound to registered nodes. Pod-bound
    /// tokens with complete claims use the reviewed node name and UID. Missing
    /// claims and static-token callers retain the registered-address binding;
    /// malformed claims or a mismatched UID never fall back. A caller with no
    /// matching registered node resolves
    /// generic templates only. A request that reached the handler without the
    /// auth layer (tests, local simulators) is unscoped, matching how the
    /// other narrow authorities treat a missing extension.
    async fn template_read_scope<T>(
        &self,
        request: &Request<T>,
    ) -> Result<super::template_releases_store::TemplateReadScope, Status> {
        use super::template_releases_store::TemplateReadScope;
        use pplx_space_common_svc::auth::{
            AuthenticatedNodeIdentity, NodeApiAuthenticatedCaller, TemplateReleaseReadAuthority,
        };
        use pplx_space_metrics::auth::{TemplateNodeBinding, template_node_binding};

        let Some(caller) = request.extensions().get::<NodeApiAuthenticatedCaller>() else {
            return Ok(TemplateReadScope::Unscoped);
        };
        if caller.template_release_read_authority() == TemplateReleaseReadAuthority::Unscoped {
            return Ok(TemplateReadScope::Unscoped);
        }
        match caller.node_identity() {
            AuthenticatedNodeIdentity::Kubernetes(node) => {
                let scope = self
                    .template_releases_store
                    .node_scope_for_identity(node)
                    .await
                    .map_err(|err| {
                        Status::internal(format!("resolve caller node identity: {err}"))
                    })?;
                if matches!(&scope, TemplateReadScope::NodeScoped { node_ids } if node_ids.is_empty())
                {
                    template_node_binding(TemplateNodeBinding::NoInventoryMatch);
                    tracing::warn!(node_name = %node.name(), node_uid = %node.uid(),
                        "template catalog node identity has no live inventory match; private templates are unavailable");
                } else {
                    template_node_binding(TemplateNodeBinding::Bound);
                }
                return Ok(scope);
            }
            AuthenticatedNodeIdentity::MalformedKubernetesClaims => {
                template_node_binding(TemplateNodeBinding::Unbound);
                tracing::warn!(
                    peer_addr = ?request.remote_addr(),
                    "template catalog caller has malformed pod/node claims; private templates are unavailable"
                );
                return Ok(TemplateReadScope::NodeScoped {
                    node_ids: Vec::new(),
                });
            }
            AuthenticatedNodeIdentity::MissingKubernetesClaims => {
                template_node_binding(TemplateNodeBinding::AddressFallback);
            }
            AuthenticatedNodeIdentity::RemoteAddress => {}
        }
        let Some(remote_addr) = request.remote_addr() else {
            return Ok(TemplateReadScope::NodeScoped {
                node_ids: Vec::new(),
            });
        };
        self.template_releases_store
            .node_scope_for_address(normalize_ip(remote_addr.ip()))
            .await
            .map_err(|err| Status::internal(format!("resolve caller nodes: {err}")))
    }
}

#[tonic::async_trait]
impl ClusterApi for ClusterApiService {
    async fn create_system_template_publication_grant(
        &self,
        request: Request<CreateSystemTemplatePublicationGrantRequest>,
    ) -> Result<Response<CreateSystemTemplatePublicationGrantResponse>, Status> {
        pplx_space_common_svc::auth::build::authorize_system_publication_grant(&request)?;
        let template = template_publication::parse_template_name(&request.get_ref().template_name)?;
        let mut tx = self.pool.begin().await.map_err(|error| {
            Status::internal(format!("begin system publication grant: {error}"))
        })?;
        let grant = template_system_publication_grants::issue(&mut tx, &template)
            .await
            .map_err(|error| {
                Status::internal(format!("issue system publication grant: {error}"))
            })?;
        tx.commit().await.map_err(|error| {
            Status::internal(format!("commit system publication grant: {error}"))
        })?;
        Ok(Response::new(
            CreateSystemTemplatePublicationGrantResponse {
                grant_id: grant.to_string(),
            },
        ))
    }

    async fn list_nodes(
        &self,
        request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let request = request.into_inner();
        let role = request.role.map(db_role_from_proto).transpose()?;
        // Inventory includes both substrates.
        let mut rows = self.fetch_nodes(role, request.include_deleted).await?;
        rows.extend(
            crate::node_inventory::list_kubernetes(&self.pool, role, request.include_deleted)
                .await
                .map_err(|error| Status::internal(format!("query Kubernetes nodes: {error}")))?,
        );
        let nodes = rows
            .into_iter()
            .map(node_record)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(ListNodesResponse { nodes }))
    }

    async fn register_template(
        &self,
        request: Request<RegisterTemplateRequest>,
    ) -> Result<Response<RegisterTemplateResponse>, Status> {
        pplx_space_common_svc::auth::build::authorize_template_build(
            &request,
            request.get_ref().org_id.as_deref(),
        )?;
        let requires_grant =
            pplx_space_common_svc::auth::build::system_publication_requires_grant(&request);
        let requires_tenant_grant =
            pplx_space_common_svc::auth::build::tenant_publication_requires_grant(&request);
        let publisher_addr = request.remote_addr();
        let publisher_node =
            pplx_space_common_svc::auth::build::publication_node(&request).cloned();
        let request = request.into_inner();
        let name = template_publication::parse_template_name(&request.name)?;
        let description = optional_trimmed(request.description.as_deref());
        let template_org = TemplateOrg::parse(request.org_id.as_deref())?;
        let tenant_grant = template_publication::tenant_publication_grant(
            request.tenant_publication_grant_id.as_deref(),
            template_org,
            requires_tenant_grant,
        )?;
        let system_grant = template_publication::system_publication_grant(
            request.system_publication_grant_id.as_deref(),
            template_org,
            requires_grant,
        )?;
        self.reserved_template_names
            .authorize(&name, template_org)?;
        let publisher_nodes = match tenant_grant {
            Some(_) => {
                self.publisher_nodes(publisher_addr, publisher_node.as_ref())
                    .await?
            }
            None => Vec::new(),
        };

        let mut tx = self.pool.begin().await.map_err(|err| {
            template_publication::storage_status("begin template registration", &err)
        })?;

        // Registration leaves the grant for the release that follows it.
        if let Some(grant) = system_grant
            && !template_system_publication_grants::covers(&mut *tx, &name, grant)
                .await
                .map_err(|err| {
                    template_publication::storage_status("check system publication", err.as_sql())
                })?
        {
            return Err(template_publication::missing_system_publication_grant());
        }
        if let (Some(grant), TemplateOrg::Owned { org_id }) = (tenant_grant, template_org)
            && !template_tenant_publication_grants::covers(
                &mut *tx,
                &template_tenant_publication_grants::TenantPublication {
                    grant,
                    template: &name,
                    org_id,
                    caller_nodes: &publisher_nodes,
                },
            )
            .await
            .map_err(|err| {
                template_publication::storage_status("check tenant publication", err.as_sql())
            })?
        {
            return Err(template_publication::missing_tenant_publication_grant());
        }

        let inserted = template_registration_store::insert(
            &mut *tx,
            name.as_str(),
            description.as_deref(),
            template_org.org_id(),
        )
        .await
        .map_err(|err| template_publication::storage_status("insert template", &err))?;
        let row = match inserted {
            Some(row) => row,
            None => {
                self.lock_template_for_publication(&mut tx, &name, template_org)
                    .await?;
                template_registration_store::fill_description(
                    &mut *tx,
                    name.as_str(),
                    description.as_deref(),
                )
                .await
                .map_err(|err| {
                    template_publication::storage_status("update template description", &err)
                })?
            }
        };

        tx.commit().await.map_err(|err| {
            template_publication::storage_status("commit template registration", &err)
        })?;

        Ok(Response::new(RegisterTemplateResponse {
            template: Some(template_proto(row)),
        }))
    }

    async fn register_template_release(
        &self,
        request: Request<RegisterTemplateReleaseRequest>,
    ) -> Result<Response<RegisterTemplateReleaseResponse>, Status> {
        pplx_space_common_svc::auth::build::authorize_template_build(
            &request,
            request.get_ref().org_id.as_deref(),
        )?;
        let requires_grant =
            pplx_space_common_svc::auth::build::system_publication_requires_grant(&request);
        let requires_tenant_grant =
            pplx_space_common_svc::auth::build::tenant_publication_requires_grant(&request);
        let publisher_addr = request.remote_addr();
        let publisher_node =
            pplx_space_common_svc::auth::build::publication_node(&request).cloned();
        let request = request.into_inner();
        let template_name = template_publication::parse_template_name(&request.template_name)?;
        let runtime = validated_template_runtime(&request.runtime)?;
        let hash = validated_release_hash(&request.hash)?;
        let tags = dedupe_required_strings(request.tags, "tag")?;
        let storage_locations = validated_storage_connection_strings(
            request.storage_connection_strings,
            &self.trusted_template_storage,
        )?;
        let template_org = TemplateOrg::parse(request.org_id.as_deref())?;
        let tenant_grant = template_publication::tenant_publication_grant(
            request.tenant_publication_grant_id.as_deref(),
            template_org,
            requires_tenant_grant,
        )?;
        let system_grant = template_publication::system_publication_grant(
            request.system_publication_grant_id.as_deref(),
            template_org,
            requires_grant,
        )?;
        self.reserved_template_names
            .authorize(&template_name, template_org)?;
        let publisher_nodes = match tenant_grant {
            Some(_) => {
                self.publisher_nodes(publisher_addr, publisher_node.as_ref())
                    .await?
            }
            None => Vec::new(),
        };
        let execution_metadata = validated_execution_metadata(request.execution.as_ref())?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| template_publication::storage_status("begin tx", &err))?;

        // Consumed with the release it authorizes: a rolled-back publication
        // keeps its grant, and a committed one cannot be repeated.
        if let Some(grant) = system_grant
            && !template_system_publication_grants::consume(&mut *tx, &template_name, grant)
                .await
                .map_err(|err| {
                    template_publication::storage_status("consume system publication", err.as_sql())
                })?
        {
            return Err(template_publication::missing_system_publication_grant());
        }
        if let (Some(grant), TemplateOrg::Owned { org_id }) = (tenant_grant, template_org)
            && !template_tenant_publication_grants::consume(
                &mut *tx,
                &template_tenant_publication_grants::TenantPublication {
                    grant,
                    template: &template_name,
                    org_id,
                    caller_nodes: &publisher_nodes,
                },
            )
            .await
            .map_err(|err| {
                template_publication::storage_status("consume tenant publication", err.as_sql())
            })?
        {
            return Err(template_publication::missing_tenant_publication_grant());
        }
        let release_org_id = self
            .lock_template_for_publication(&mut tx, &template_name, template_org)
            .await?;
        let release = ensure_template_release(
            &mut tx,
            template_name.as_str(),
            runtime,
            &hash,
            release_org_id,
            execution_metadata.as_ref(),
        )
        .await?;

        let mut tag_rows = Vec::with_capacity(tags.len());
        for tag in &tags {
            let row = ensure_template_release_tag(&mut tx, &release, tag).await?;
            tag_rows.push(row);
        }

        let mut location_rows = Vec::with_capacity(storage_locations.len());
        for connection_string in &storage_locations {
            let row = ensure_template_release_storage_location(
                &mut tx,
                &release,
                connection_string.as_wire(),
            )
            .await?;
            location_rows.push(row);
        }

        tx.commit()
            .await
            .map_err(|err| template_publication::storage_status("commit tx", &err))?;

        Ok(Response::new(RegisterTemplateReleaseResponse {
            release: Some(template_release_proto(release)),
            tags: tag_rows
                .into_iter()
                .map(template_release_tag_proto)
                .collect(),
            storage_locations: location_rows
                .into_iter()
                .map(template_release_storage_location_proto)
                .collect(),
        }))
    }

    async fn find_template_release(
        &self,
        request: Request<FindTemplateReleaseRequest>,
    ) -> Result<Response<FindTemplateReleaseResponse>, Status> {
        let scope = self.template_read_scope(&request).await?;
        let delegation = TemplateLookupScope::from_request(&request)?;
        if delegation.is_none() {
            pplx_space_metrics::auth::template_lookup_compatibility("missing_scope");
        }
        let request = request.into_inner();
        let template_name = trimmed_required(&request.template_name, "template_name")?;
        let runtime = validated_template_runtime(&request.runtime)?;
        let tag = trimmed_required(&request.tag, "tag")?;

        // Authorization, release, and storage locations share one DB snapshot.
        // The cached visibility appended below is advisory, never an auth gate.
        let caller_org = request
            .caller_org_id
            .as_deref()
            .map(str::parse::<OrgId>)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let rows = self
            .template_releases_store
            .find_by_tag(
                &template_name,
                runtime,
                &tag,
                caller_org,
                &scope,
                request.generic_only,
            )
            .await
            .map_err(|error| Status::internal(format!("query release: {error}")))?;

        let generic_authorized = rows.first().is_some_and(|row| row.generic_authorized);
        let release = match rows.first() {
            Some(first) => TemplateRelease {
                id: first.release_id.to_string(),
                template_name: first.release_template_name.clone(),
                runtime: first.release_type.clone(),
                hash: first.release_hash.clone(),
                created_at: format_time(first.release_created_at),
            },
            None => {
                return Err(Status::not_found("template release not found"));
            }
        };

        let storage_locations = rows
            .into_iter()
            .filter_map(|row| {
                let id = row.storage_id?;
                let connection_string = row.storage_connection_string?;
                let created_at = row.storage_created_at?;
                Some(TemplateReleaseStorageLocation {
                    id: id.to_string(),
                    connection_string,
                    created_at: format_time(created_at),
                })
            })
            .collect();

        let visibility = template_visibility(
            &self.visibility_pool,
            &self.template_visibility_cache,
            &self.visibility_observations,
            &template_name,
        )
        .await;
        let mut response = Response::new(FindTemplateReleaseResponse {
            generic_authorized,
            authorized_org_id: caller_org.map(|org| org.to_string()),
            release: Some(release),
            storage_locations,
            template_visibility: visibility.into(),
        });
        if let Some(scope) = delegation {
            scope.receipt(&mut response);
        }
        Ok(response)
    }

    async fn find_template_release_by_hash(
        &self,
        request: Request<FindTemplateReleaseByHashRequest>,
    ) -> Result<Response<FindTemplateReleaseResponse>, Status> {
        let scope = self.template_read_scope(&request).await?;
        let delegation = TemplateLookupScope::from_request(&request)?;
        if delegation.is_none() {
            pplx_space_metrics::auth::template_lookup_compatibility("missing_scope");
        }
        let request = request.into_inner();
        let template_name = trimmed_required(&request.template_name, "template_name")?;
        // Canonical `sha256:`-prefixed form.
        let hash = validated_release_hash(&request.hash)?;
        // Bare form for tolerant column matching: `template_releases.hash`
        // is observed both bare and `sha256:`-prefixed, so match either
        // shape rather than rejecting a digest stored in the other form.
        let bare_hash = hash
            .strip_prefix(&format!(
                "{}:",
                pplx_space_common_core::types::HashAlg::Sha256.as_str()
            ))
            .unwrap_or(&hash)
            .to_string();

        // Digests resolve without a tag and authorize in the same statement.
        // Artifact digests already identify the runtime variant.
        let caller_org = request
            .caller_org_id
            .as_deref()
            .map(str::parse::<OrgId>)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let rows = self
            .template_releases_store
            .find_by_hash(
                &template_name,
                &hash,
                &bare_hash,
                caller_org,
                &scope,
                request.generic_only,
            )
            .await
            .map_err(|error| Status::internal(format!("query release: {error}")))?;

        let generic_authorized = rows.first().is_some_and(|row| row.generic_authorized);
        let release = match rows.first() {
            Some(first) => TemplateRelease {
                id: first.release_id.to_string(),
                template_name: first.release_template_name.clone(),
                runtime: first.release_type.clone(),
                hash: first.release_hash.clone(),
                created_at: format_time(first.release_created_at),
            },
            None => {
                return Err(Status::not_found("template release not found"));
            }
        };

        let storage_locations = rows
            .into_iter()
            .filter_map(|row| {
                let id = row.storage_id?;
                let connection_string = row.storage_connection_string?;
                let created_at = row.storage_created_at?;
                Some(TemplateReleaseStorageLocation {
                    id: id.to_string(),
                    connection_string,
                    created_at: format_time(created_at),
                })
            })
            .collect();

        let visibility = template_visibility(
            &self.visibility_pool,
            &self.template_visibility_cache,
            &self.visibility_observations,
            &template_name,
        )
        .await;
        let mut response = Response::new(FindTemplateReleaseResponse {
            generic_authorized,
            authorized_org_id: caller_org.map(|org| org.to_string()),
            release: Some(release),
            storage_locations,
            template_visibility: visibility.into(),
        });
        if let Some(scope) = delegation {
            scope.receipt(&mut response);
        }
        Ok(response)
    }

    async fn list_latest_template_releases(
        &self,
        request: Request<ListLatestTemplateReleasesRequest>,
    ) -> Result<Response<ListLatestTemplateReleasesResponse>, Status> {
        let scope = self.template_read_scope(&request).await?;
        let request = request.into_inner();
        let template_name = trimmed_required(&request.template_name, "template_name")?;
        let runtime = validated_template_runtime(&request.runtime)?;
        let limit = validated_list_limit(request.limit)?;

        let releases = self
            .template_releases_store
            .list_latest(&template_name, runtime, i64::from(limit), &scope)
            .await?;

        let visibility = template_visibility(
            &self.visibility_pool,
            &self.template_visibility_cache,
            &self.visibility_observations,
            &template_name,
        )
        .await;
        let entries = releases
            .into_iter()
            .map(|release| ListLatestTemplateReleasesEntry {
                release: Some(TemplateRelease {
                    id: release.id.to_string(),
                    template_name: release.template_name,
                    runtime: release.runtime,
                    hash: release.hash,
                    created_at: format_time(release.created_at),
                }),
                storage_locations: release
                    .storage_locations
                    .into_iter()
                    .map(|storage| TemplateReleaseStorageLocation {
                        id: storage.id,
                        connection_string: storage.connection_string,
                        created_at: storage.created_at,
                    })
                    .collect(),
                template_visibility: visibility.into(),
            })
            .collect();

        Ok(Response::new(ListLatestTemplateReleasesResponse {
            entries,
            template_visibility: visibility.into(),
        }))
    }

    async fn list_warm_pool_template_releases(
        &self,
        _request: Request<ListWarmPoolTemplateReleasesRequest>,
    ) -> Result<Response<ListWarmPoolTemplateReleasesResponse>, Status> {
        // Two sources, deduped by the pool store:
        //  - every release an *enabled* kubernetes_pool_configs row points
        //    at (the pools that exist right now), and
        //  - the retained-N release window of each enabled
        //    warm_pool_policies row (the releases the pool autoscaler may
        //    target next).
        // Releases demanded by observed sandbox traffic but not yet in
        // either set are intentionally absent: such builds are in active
        // use, so the GC's active-set and grace protections cover them
        // until the autoscaler materializes a config row.
        //
        // The `c.enabled` filter is load-bearing: a drained pool (its
        // config kept as `enabled = FALSE`, `target_ready_slots = 0` after
        // the autoscaler stops targeting a superseded release) targets
        // nothing, so it must not pin its build. Without it, every drained
        // release accumulates as a permanent GC pin — a frequently
        // republished template piles up hundreds of disabled configs and
        // its on-node builds are never reclaimed, filling `/space/cache`.
        let rows = pplx_space_cluster_pool::WarmPoolStore::new(self.pool.clone())
            .protected_template_releases()
            .await
            .map_err(|err| Status::internal(format!("query warm pool releases: {err}")))?;

        Ok(Response::new(ListWarmPoolTemplateReleasesResponse {
            releases: rows
                .into_iter()
                .map(|release| WarmPoolTemplateRelease {
                    template_name: release.template_name,
                    hash: release.content_hash,
                })
                .collect(),
        }))
    }

    async fn get_node_network_policy_state(
        &self,
        request: Request<GetNodeNetworkPolicyStateRequest>,
    ) -> Result<Response<GetNodeNetworkPolicyStateResponse>, Status> {
        let role_hint = optional_db_role_from_proto(request.get_ref().role)?;
        let origin = self
            .authenticated_origin_node_id(&request, &request.get_ref().node_id, role_hint)
            .await?;
        let request = request.into_inner();
        let templates = dedupe_optional_strings(request.templates);
        let sandbox_ids = parse_sandbox_ids(request.sandbox_ids)?;

        let template_rows = if templates.is_empty() {
            Vec::new()
        } else {
            sqlx::query_as::<_, TemplatePolicyRow>(sql!(
                "WITH requested AS (
                     SELECT DISTINCT UNNEST($1::TEXT[]) AS template
                 )
                 SELECT r.template,
                        COALESCE(t.network_policy_json, $2::JSONB) AS network_policy_json,
                        COALESCE(t.network_policy_generation, 0) AS network_policy_generation
                 FROM requested r
                 LEFT JOIN templates t ON t.name = r.template
                 WHERE (t.visibility = 'generic' AND t.desired_state = 'present')
                    OR EXISTS (
                        SELECT 1
                        FROM sandboxes sb
                        WHERE sb.template_id = t.name
                          AND (sb.active_node_id = $3 OR sb.target_node_id = $3)
                    )
                    OR ($4 = 'worker' AND EXISTS (
                        SELECT 1
                        FROM kubernetes_pool_configs pc
                        JOIN template_releases pr ON pr.id = pc.template_release_id
                        WHERE pc.enabled
                          AND pr.template_name = t.name
                    ))
                    OR ($4 = 'builder' AND EXISTS (
                        SELECT 1
                        FROM template_build_base_grants g
                        WHERE g.base_template_name = t.name
                          AND g.org_id = t.org_id
                          AND ((g.builder_node_id IS NULL
                                AND g.granted_at > NOW() - INTERVAL '1 hour')
                               OR (g.builder_node_id = $3
                                   AND g.granted_at > NOW() - INTERVAL '7 days'))
                    ))"
            ))
            .bind(&templates)
            .bind(Json(NetworkPolicyV1::deny_all()))
            .bind(origin.id)
            .bind(origin.role.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|err| Status::internal(format!("query template policies: {err}")))?
        };
        let mut template_by_name = std::collections::BTreeMap::new();
        for row in template_rows {
            template_by_name.insert(row.template.clone(), row);
        }

        let template_states = templates
            .into_iter()
            .filter_map(|template| {
                let row = match template_by_name.get(&template) {
                    Some(row) => row,
                    None => {
                        tracing::warn!(
                            template,
                            "node requested network policy for unknown template"
                        );
                        return None;
                    }
                };
                Some(TemplateNetworkPolicyState {
                    template,
                    policy: Some(common_network_policy_to_proto(&row.network_policy_json.0)),
                    generation: db_generation_to_proto(row.network_policy_generation),
                })
            })
            .collect();

        let sandbox_states = if sandbox_ids.is_empty() {
            Vec::new()
        } else {
            let rows = pplx_space_sandbox_store::network_policy_inputs_placed_on(
                &self.pool,
                origin.id,
                &sandbox_ids,
            )
            .await
            .map_err(|err| Status::internal(format!("query sandbox policies: {err}")))?
            .into_iter()
            .map(SandboxPolicyRow::try_from)
            .collect::<Result<Vec<_>, Status>>()?;

            project_sandbox_network_policy_states(rows)
        };

        Ok(Response::new(GetNodeNetworkPolicyStateResponse {
            templates: template_states,
            sandboxes: sandbox_states,
        }))
    }

    async fn get_node_template_base_snapshot_policy_state(
        &self,
        request: Request<GetNodeTemplateBaseSnapshotPolicyStateRequest>,
    ) -> Result<Response<GetNodeTemplateBaseSnapshotPolicyStateResponse>, Status> {
        let role_hint = optional_db_role_from_proto(request.get_ref().role)?;
        let origin = self
            .authenticated_origin_node_id(&request, &request.get_ref().node_id, role_hint)
            .await?;
        let request = request.into_inner();
        let templates = dedupe_optional_strings(request.templates);
        if templates.is_empty() {
            return Ok(Response::new(
                GetNodeTemplateBaseSnapshotPolicyStateResponse {
                    templates: Vec::new(),
                },
            ));
        }

        let rows = sqlx::query_as::<_, NodeTemplateBaseSnapshotPolicyRow>(sql!(
            "WITH requested AS (
                 SELECT DISTINCT UNNEST($1::TEXT[]) AS template
             ),
             local_inventory AS (
                 SELECT DISTINCT ON (nti.template_name)
                        nti.template_name,
                        nti.vcpu,
                        nti.memory_mib
                 FROM node_template_inventory nti
                 WHERE nti.node_id = $2
                   AND (nti.vcpu IS NOT NULL OR nti.memory_mib IS NOT NULL)
                 ORDER BY nti.template_name,
                          nti.observed_at DESC,
                          nti.built_at DESC NULLS LAST,
                          nti.content_hash ASC
             )
             SELECT r.template,
                    COALESCE(p.ensure_base_snapshot, FALSE) AS ensure_base_snapshot,
                    COALESCE(li.vcpu, p.snapshot_vcpu) AS snapshot_vcpu,
                    COALESCE(li.memory_mib, p.snapshot_memory_mib) AS snapshot_memory_mib
             FROM requested r
             LEFT JOIN template_base_snapshot_policies p ON p.template = r.template
             LEFT JOIN local_inventory li ON li.template_name = r.template
             LEFT JOIN templates t ON t.name = r.template
             WHERE (t.visibility = 'generic' AND t.desired_state = 'present')
                OR EXISTS (
                    SELECT 1
                    FROM sandboxes sb
                    WHERE sb.template_id = t.name
                      AND (sb.active_node_id = $2 OR sb.target_node_id = $2)
                )
                OR ($3 = 'worker' AND EXISTS (
                    SELECT 1
                    FROM kubernetes_pool_configs pc
                    JOIN template_releases pr ON pr.id = pc.template_release_id
                    WHERE pc.enabled
                      AND pr.template_name = t.name
                ))
                OR ($3 = 'builder' AND EXISTS (
                    SELECT 1
                    FROM template_build_base_grants g
                    WHERE g.base_template_name = t.name
                      AND g.org_id = t.org_id
                      AND ((g.builder_node_id IS NULL
                            AND g.granted_at > NOW() - INTERVAL '1 hour')
                           OR (g.builder_node_id = $2
                               AND g.granted_at > NOW() - INTERVAL '7 days'))
                ))"
        ))
        .bind(&templates)
        .bind(origin.id)
        .bind(origin.role.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Status::internal(format!("query template base-snapshot policies: {err}")))?;

        let mut by_name = std::collections::BTreeMap::new();
        for row in rows {
            by_name.insert(row.template.clone(), row);
        }
        let states = templates
            .into_iter()
            .filter_map(|template| {
                let row = match by_name.get(&template) {
                    Some(row) => row,
                    None => {
                        tracing::warn!(
                            template,
                            "node requested base-snapshot policy for unknown template"
                        );
                        return None;
                    }
                };
                let shape = BaseSnapshotShape::from_optional_i32(
                    row.snapshot_vcpu,
                    row.snapshot_memory_mib,
                );
                Some(TemplateBaseSnapshotPolicyState {
                    template,
                    ensure_base_snapshot: row.ensure_base_snapshot,
                    snapshot_vcpu: shape.vcpu.get().get(),
                    snapshot_memory_mib: u32::try_from(shape.memory.as_mib_floor())
                        .expect("base snapshot memory is derived from i32 MiB"),
                })
            })
            .collect();

        Ok(Response::new(
            GetNodeTemplateBaseSnapshotPolicyStateResponse { templates: states },
        ))
    }

    async fn get_node_runtime_desired_state(
        &self,
        request: Request<GetNodeRuntimeDesiredStateRequest>,
    ) -> Result<Response<GetNodeRuntimeDesiredStateResponse>, Status> {
        let role_hint = optional_db_role_from_proto(request.get_ref().role)?;
        self.authenticated_origin_node_id(&request, &request.get_ref().node_id, role_hint)
            .await?;
        // No provisioning policy store is configured. Absence must not become
        // an implicit E0 installation or permission to delete existing files.
        Ok(Response::new(GetNodeRuntimeDesiredStateResponse {
            recipe: None,
        }))
    }

    async fn get_node_template_desired_state(
        &self,
        request: Request<GetNodeTemplateDesiredStateRequest>,
    ) -> Result<Response<GetNodeTemplateDesiredStateResponse>, Status> {
        let role_hint = optional_db_role_from_proto(request.get_ref().role)?;
        let origin = self
            .authenticated_origin_node_id(&request, &request.get_ref().node_id, role_hint)
            .await?;
        let request = request.into_inner();
        let templates = dedupe_optional_strings(request.templates);
        if templates.is_empty() {
            return Ok(Response::new(GetNodeTemplateDesiredStateResponse {
                templates: Vec::new(),
            }));
        }

        // Visibility follows the node's placement, as the release lookups do.
        // The one addition is a template the node still holds while it is
        // being retired: the node must hear `present = false` to delete it.
        // Inventory is node-reported, so it never admits a present template.
        let rows = sqlx::query_as::<_, TemplateDesiredStateRow>(
            sql!("WITH requested AS (
                 SELECT DISTINCT UNNEST($1::TEXT[]) AS template
             ),
             local_inventory AS (
                 SELECT DISTINCT ON (nti.template_name)
                        nti.template_name,
                        nti.vcpu,
                        nti.memory_mib
                 FROM node_template_inventory nti
                 WHERE nti.node_id = $3
                   AND (nti.vcpu IS NOT NULL OR nti.memory_mib IS NOT NULL)
                 ORDER BY nti.template_name,
                          nti.observed_at DESC,
                          nti.built_at DESC NULLS LAST,
                          nti.content_hash ASC
             )
             SELECT r.template,
                    t.desired_state,
                    al.default_auto_lifecycle_json,
                    CASE
                        WHEN $2 = 'worker' AND t.desired_state = 'present'
                        THEN pp.target_count
                        ELSE NULL
                    END AS pool_target,
                    CASE
                        WHEN $2 = 'worker' AND t.desired_state = 'present'
                        THEN pp.boot_mode
                        ELSE NULL
                    END AS pool_boot_mode,
                    CASE
                        WHEN $2 = 'worker' AND t.desired_state = 'present' AND pp.boot_mode = 'snapshot'
                        THEN COALESCE(li.vcpu, bsp.snapshot_vcpu)
                        ELSE NULL
                    END AS pool_vcpu,
                    CASE
                        WHEN $2 = 'worker' AND t.desired_state = 'present' AND pp.boot_mode = 'snapshot'
                        THEN COALESCE(li.memory_mib, bsp.snapshot_memory_mib)
                        ELSE NULL
                    END AS pool_memory_mib
             FROM requested r
             JOIN templates t ON t.name = r.template
             LEFT JOIN template_default_auto_lifecycle_policies al
                    ON al.template = r.template
             LEFT JOIN template_pool_policies pp
                    ON pp.template = r.template
             LEFT JOIN template_base_snapshot_policies bsp
                    ON bsp.template = r.template
             LEFT JOIN local_inventory li
                    ON li.template_name = r.template
             WHERE (t.visibility = 'generic' AND t.desired_state = 'present')
                OR EXISTS (
                    SELECT 1
                    FROM sandboxes sb
                    WHERE sb.template_id = t.name
                      AND (sb.active_node_id = $3 OR sb.target_node_id = $3)
                )
                OR ($2 = 'worker' AND EXISTS (
                    SELECT 1
                    FROM kubernetes_pool_configs pc
                    JOIN template_releases pr ON pr.id = pc.template_release_id
                    WHERE pc.enabled
                      AND pr.template_name = t.name
                ))
                OR ($2 = 'builder' AND EXISTS (
                    SELECT 1
                    FROM template_build_base_grants g
                    WHERE g.base_template_name = t.name
                      AND g.org_id = t.org_id
                      AND ((g.builder_node_id IS NULL
                            AND g.granted_at > NOW() - INTERVAL '1 hour')
                           OR (g.builder_node_id = $3
                               AND g.granted_at > NOW() - INTERVAL '7 days'))
                ))
                OR (t.desired_state <> 'present' AND EXISTS (
                    SELECT 1
                    FROM node_template_inventory nti
                    WHERE nti.template_name = r.template
                      AND nti.node_id = $3
                ))"),
        )
        .bind(&templates)
        .bind(origin.role.as_str())
        .bind(origin.id)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Status::internal(format!("query template desired state: {err}")))?;

        let mut by_name = std::collections::BTreeMap::new();
        for row in rows {
            by_name.insert(row.template.clone(), row);
        }
        let mut states = Vec::new();
        for template in templates {
            let Some(row) = by_name.get(&template) else {
                tracing::warn!(
                    template,
                    "node requested desired state for unknown template"
                );
                continue;
            };
            let desired_state = row
                .desired_state
                .parse::<DomainTemplateDesiredState>()
                .map_err(|error| {
                    Status::internal(format!("stored template desired state is invalid: {error}"))
                })?;
            let present = desired_state == DomainTemplateDesiredState::Present;
            let pool = if present {
                match (row.pool_target, row.pool_boot_mode.as_deref()) {
                    (Some(target_count), Some(boot_mode)) => {
                        let shape = if boot_mode == "snapshot" {
                            let shape = BaseSnapshotShape::from_optional_i32(
                                row.pool_vcpu,
                                row.pool_memory_mib,
                            );
                            (
                                Some(shape.vcpu.get().get()),
                                Some(
                                    u32::try_from(shape.memory.as_mib_floor())
                                        .expect("base snapshot memory is derived from i32 MiB"),
                                ),
                            )
                        } else {
                            (None, None)
                        };
                        Some(TemplatePoolPolicyState {
                            target_count: u32::try_from(target_count).unwrap_or(0),
                            boot_mode: boot_mode.to_string(),
                            vcpu: shape.0,
                            memory_mib: shape.1,
                        })
                    }
                    _ => None,
                }
            } else {
                None
            };
            states.push(TemplateDesiredState {
                template,
                default_auto_lifecycle: row
                    .default_auto_lifecycle_json
                    .as_ref()
                    .filter(|_| present)
                    .map(|policy| common_auto_lifecycle_to_proto(&policy.0)),
                pool,
                present: Some(present),
            });
        }

        Ok(Response::new(GetNodeTemplateDesiredStateResponse {
            templates: states,
        }))
    }

    async fn get_node_sandbox_desired_state(
        &self,
        request: Request<GetNodeSandboxDesiredStateRequest>,
    ) -> Result<Response<GetNodeSandboxDesiredStateResponse>, Status> {
        let role_hint =
            db_role_from_proto_or_default(request.get_ref().role, SpaceNodeRole::Worker)?;
        if role_hint != SpaceNodeRole::Worker && role_hint != SpaceNodeRole::Builder {
            return Err(Status::invalid_argument(
                "sandbox desired state can only be fetched by worker or builder nodes",
            ));
        }
        let origin = self
            .authenticated_origin_node_id(&request, &request.get_ref().node_id, Some(role_hint))
            .await?;
        let request = request.into_inner();
        let sandbox_ids = parse_sandbox_ids(request.sandbox_ids)?;
        if sandbox_ids.is_empty() {
            return Ok(Response::new(GetNodeSandboxDesiredStateResponse {
                sandboxes: Vec::new(),
                deleted_sandbox_ids: Vec::new(),
            }));
        }

        // Unclaimed warm-pool slots reserve their sandbox id before any
        // `sandboxes` row exists (the row is inserted at claim), so they
        // must not be reported deleted: without this anti-join every
        // pool-hosting node would carry a permanent false-positive
        // baseline in `deleted_sandbox_ids`. Read the reservations BEFORE
        // the sandboxes read — a slot claimed between the two reads is
        // then present in at least one of them, never in neither.
        let pool_reserved = pplx_space_cluster_pool::WarmPoolStore::new(self.pool.clone())
            .reserved_sandbox_ids(&sandbox_ids)
            .await
            .map_err(|err| Status::internal(format!("query pool slot reservations: {err}")))?;
        let pool_reserved = pool_reserved
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();

        // Deliberately unfiltered by node: one consistent read partitions
        // the requested ids into assigned-here (desired state returned),
        // exists-elsewhere/unplaced (returned in neither list), and
        // no-row-at-all (`deleted_sandbox_ids` — `sandboxes` rows are
        // hard-deleted on delete, and a node only asks about ids it has
        // local state for, so a missing row means the sandbox was deleted
        // and its node-local presence is unreclaimable dead weight). The
        // payload columns are projected only for assigned-here rows; other
        // rows contribute 17 bytes to the partition instead of their
        // jsonb policy.
        let rows = sqlx::query_as::<_, SandboxDesiredStateRow>(sql!(
            "SELECT id AS sandbox_id,
                    (active_node_id = $2) IS TRUE AS assigned_here,
                    CASE WHEN active_node_id = $2
                         THEN snapshots_enabled_desired END AS snapshots_enabled_desired,
                    CASE WHEN active_node_id = $2
                         THEN auto_lifecycle_json END AS auto_lifecycle_json
             FROM sandboxes
             WHERE id = ANY($1)"
        ))
        .bind(&sandbox_ids)
        .bind(origin.id)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Status::internal(format!("query sandbox desired state: {err}")))?;

        // Steady-state skip: an open row whose last observed bridge status
        // is healthy (`direct` or `loopback_forwarded`), was observed
        // within the freshness window, AND was last applied on the current
        // active node is skipped. Anything else about an open row —
        // `apply_state != 'applied'`, missing `last_bridge_status`,
        // unhealthy bridge status, stale `last_observed_at`, or a
        // `last_applied_node_id` that no longer matches the active node —
        // re-includes it for the reconciler to re-apply. The active-node
        // check matters after a sandbox migration: the new node has no
        // bridge installed yet, but the DB row still carries the old
        // node's healthy observation, so without this guard the new node
        // would skip the row until the freshness window expires (~60 s of
        // 502s). Closed rows keep the simple `apply_state != 'applied'`
        // filter (no in-VM state to recover when desired closed).
        //
        // The freshness window matters because the in-VM
        // `LoopbackBridgeManager` is process-local: if `spaced-guest-svc`
        // restarts and forgets its authorized port set while the DB row
        // still says applied/healthy on this node, the next re-apply
        // detects the generation change and recovers. 60 s is a reasonable
        // upper bound on how long a 502 could persist in that scenario.
        let exposure_rows = sqlx::query_as::<_, SandboxPortExposureDesiredStateRow>(sql!(
            "SELECT spe.sandbox_id,
                    spe.protocol,
                    spe.guest_port,
                    spe.desired_state
             FROM sandbox_port_exposures spe
             JOIN sandboxes s ON s.id = spe.sandbox_id
             WHERE spe.sandbox_id = ANY($1)
               AND s.active_node_id = $2
               AND spe.source <> 'ecw'
               AND (
                   spe.apply_state != 'applied'
                   OR (
                       spe.desired_state = 'open'
                       AND (
                           spe.last_bridge_status IS NULL
                           OR spe.last_bridge_status NOT IN
                               ('direct', 'loopback_forwarded')
                           OR spe.last_observed_at IS NULL
                           OR spe.last_observed_at <
                               NOW() - INTERVAL '60 seconds'
                           OR spe.last_applied_node_id IS DISTINCT FROM $2
                       )
                   )
               )
             ORDER BY spe.sandbox_id, spe.guest_port ASC, spe.protocol ASC"
        ))
        .bind(&sandbox_ids)
        .bind(origin.id)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Status::internal(format!("query sandbox port exposures: {err}")))?;

        let mut exposures_by_sandbox =
            std::collections::BTreeMap::<SandboxId, Vec<SandboxPortExposureDesiredState>>::new();
        for row in exposure_rows {
            exposures_by_sandbox
                .entry(row.sandbox_id)
                .or_default()
                .push(SandboxPortExposureDesiredState {
                    protocol: port_protocol_to_proto(&row.protocol) as i32,
                    guest_port: db_guest_port_to_proto(row.guest_port),
                    desired_state: port_desired_state_to_proto(&row.desired_state) as i32,
                });
        }

        let mut by_id = std::collections::BTreeMap::new();
        for row in rows {
            by_id.insert(row.sandbox_id, row);
        }

        let requested_count = sandbox_ids.len();
        let mut deleted_sandbox_ids = Vec::new();
        let mut assigned_elsewhere_count = 0_usize;
        let sandboxes = sandbox_ids
            .into_iter()
            .filter_map(|sandbox_id| {
                let row = match by_id.get(&sandbox_id) {
                    Some(row) => row,
                    None => {
                        if !pool_reserved.contains(&sandbox_id) {
                            deleted_sandbox_ids.push(sandbox_id);
                        }
                        return None;
                    }
                };
                if !row.assigned_here {
                    assigned_elsewhere_count += 1;
                    return None;
                }
                Some(SandboxDesiredState {
                    sandbox_id: sandbox_id.to_string(),
                    snapshots_enabled_desired: row.snapshots_enabled_desired.unwrap_or_default(),
                    port_exposures: exposures_by_sandbox.remove(&sandbox_id).unwrap_or_default(),
                    auto_lifecycle: row
                        .auto_lifecycle_json
                        .as_ref()
                        .map(|policy| common_auto_lifecycle_to_proto(&policy.0)),
                })
            })
            .collect::<Vec<_>>();

        let suppress_threshold = deleted_partition_suppress_threshold(requested_count);
        let deleted_count = deleted_sandbox_ids.len();
        let should_suppress = deleted_partition_should_suppress(deleted_count, requested_count);
        pplx_space_metrics::reconcile::sandbox_desired_state_deleted_reported(
            &origin.display_id,
            deleted_count as u64,
        );
        if !deleted_partition_enabled() {
            deleted_sandbox_ids.clear();
        } else if should_suppress {
            tracing::error!(
                node_id = %origin.display_id,
                requested_sandbox_count = requested_count,
                deleted_sandbox_count = deleted_count,
                suppress_threshold,
                "implausibly many requested sandbox ids have no cluster row; suppressing the \
                 deleted partition — investigate database staleness or misrouting before \
                 re-enabling cluster-deleted eviction"
            );
            pplx_space_metrics::reconcile::sandbox_desired_state_deleted_suppressed();
            deleted_sandbox_ids.clear();
        } else if !deleted_sandbox_ids.is_empty() {
            let sample_sandbox_ids = deleted_sandbox_ids
                .iter()
                .take(5)
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            tracing::debug!(
                node_id = %origin.display_id,
                requested_sandbox_count = requested_count,
                matched_sandbox_count = sandboxes.len(),
                assigned_elsewhere_sandbox_count = assigned_elsewhere_count,
                deleted_sandbox_count = deleted_count,
                sample_sandbox_ids = ?sample_sandbox_ids,
                "node holds local state for cluster-deleted sandboxes"
            );
        }

        Ok(Response::new(GetNodeSandboxDesiredStateResponse {
            sandboxes,
            deleted_sandbox_ids: deleted_sandbox_ids
                .iter()
                .map(ToString::to_string)
                .collect(),
        }))
    }

    async fn resolve_data_plane_session_route(
        &self,
        request: Request<ResolveDataPlaneSessionRouteRequest>,
    ) -> Result<Response<ResolveDataPlaneSessionRouteResponse>, Status> {
        let budget = request
            .metadata()
            .get("grpc-timeout")
            .and_then(|value| value.to_str().ok())
            .and_then(pplx_space_common_core::types::parse_grpc_timeout)
            .unwrap_or(pplx_space_common_svc::ingress_admission::preparation_timeout())
            .min(pplx_space_common_svc::ingress_admission::preparation_timeout());
        let refresh_authority = request
            .extensions()
            .get::<pplx_space_common_svc::auth::NodeApiAuthenticatedCaller>()
            .map(
                pplx_space_common_svc::auth::NodeApiAuthenticatedCaller::data_plane_session_refresh_authority,
            );
        let response = tokio::time::timeout(
            budget,
            data_plane_sessions::resolve_session_route(
                budget,
                self.ingress_activation.as_deref(),
                &self.pool,
                self.route_ed25519_authority.as_deref(),
                refresh_authority,
                request.into_inner(),
            ),
        )
        .await
        .map_err(|_| tonic::Status::deadline_exceeded("ingress preparation deadline exceeded"))??;
        Ok(Response::new(response))
    }

    async fn bind_data_plane_session_process(
        &self,
        request: Request<BindDataPlaneSessionProcessRequest>,
    ) -> Result<Response<BindDataPlaneSessionProcessResponse>, Status> {
        let response =
            data_plane_sessions::bind_session_process(&self.pool, request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn record_data_plane_session_reconnect(
        &self,
        request: Request<RecordDataPlaneSessionReconnectRequest>,
    ) -> Result<Response<RecordDataPlaneSessionReconnectResponse>, Status> {
        let response =
            data_plane_sessions::record_session_reconnect(&self.pool, request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn get_data_plane_session_revocation_state(
        &self,
        request: Request<GetDataPlaneSessionRevocationStateRequest>,
    ) -> Result<Response<GetDataPlaneSessionRevocationStateResponse>, Status> {
        let response =
            data_plane_sessions::revocation_state_for_session(&self.pool, request.into_inner())
                .await?;
        Ok(Response::new(response))
    }

    async fn unstable_resolve_sandbox_service_route(
        &self,
        request: Request<UnstableResolveSandboxServiceRouteRequest>,
    ) -> Result<Response<UnstableResolveSandboxServiceRouteResponse>, Status> {
        let budget = request
            .metadata()
            .get("grpc-timeout")
            .and_then(|value| value.to_str().ok())
            .and_then(pplx_space_common_core::types::parse_grpc_timeout)
            .unwrap_or(pplx_space_common_svc::ingress_admission::preparation_timeout())
            .min(pplx_space_common_svc::ingress_admission::preparation_timeout());
        let response = tokio::time::timeout(
            budget,
            data_plane_sessions::unstable_resolve_sandbox_service_route(
                budget,
                self.ingress_activation.as_deref(),
                &self.pool,
                self.route_ed25519_authority.as_deref(),
                &self.ecw_credential_keyring,
                request.into_inner(),
            ),
        )
        .await
        .map_err(|_| tonic::Status::deadline_exceeded("ingress preparation deadline exceeded"))??;
        Ok(Response::new(response))
    }

    async fn unstable_reset_sandbox_service_route(
        &self,
        request: Request<UnstableResetSandboxServiceRouteRequest>,
    ) -> Result<Response<UnstableResetSandboxServiceRouteResponse>, Status> {
        let response =
            data_plane_sessions::reset_sandbox_service_route(&self.pool, request.into_inner())
                .await?;
        Ok(Response::new(response))
    }

    async fn get_data_plane_org_revocation_epoch(
        &self,
        request: Request<GetDataPlaneOrgRevocationEpochRequest>,
    ) -> Result<Response<GetDataPlaneOrgRevocationEpochResponse>, Status> {
        let response =
            data_plane_sessions::org_revocation_epoch(&self.pool, request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn get_sandbox_runtime_access(
        &self,
        request: Request<GetSandboxRuntimeAccessRequest>,
    ) -> Result<Response<GetSandboxRuntimeAccessResponse>, Status> {
        let response =
            data_plane_sessions::sandbox_runtime_access(&self.pool, request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn claim_node_reconcile_refs(
        &self,
        request: Request<ClaimNodeReconcileRefsRequest>,
    ) -> Result<Response<ClaimNodeReconcileRefsResponse>, Status> {
        let role_hint = optional_db_role_from_proto(request.get_ref().role)?;
        let origin = self
            .authenticated_origin_node_id(&request, &request.get_ref().node_id, role_hint)
            .await?;
        let request = request.into_inner();
        let limit = if request.limit == 0 {
            NODE_RECONCILE_CLAIM_DEFAULT_LIMIT
        } else {
            i64::from(request.limit.min(NODE_RECONCILE_CLAIM_DEFAULT_LIMIT as u32))
        };

        let claim_token = format!("{}-{}", origin.id, Uuid::now_v7());
        let rows = sqlx::query_as::<_, ClaimedFacetRefRow>(sql!(
            "WITH picked AS (
                 SELECT node_id, facet, element, requested_at
                 FROM node_reconcile_queue
                 WHERE node_id = $1
                   AND next_attempt_at <= NOW()
                   AND dead_at IS NULL
                   AND (
                       claim_token = ''
                       OR claimed_at <= NOW() - ($3 * INTERVAL '1 second')
                   )
                 ORDER BY requested_at ASC, facet ASC, element ASC
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             UPDATE node_reconcile_queue q
             SET claim_token = $4,
                 claim_generation = q.generation,
                 claimed_at = NOW(),
                 updated_at = NOW()
             FROM picked
             WHERE q.node_id = picked.node_id
               AND q.facet = picked.facet
               AND q.element = picked.element
             RETURNING q.facet,
                       NULLIF(q.element, '') AS element,
                       q.traceparent,
                       picked.requested_at"
        ))
        .bind(origin.id)
        .bind(limit)
        .bind(NODE_RECONCILE_CLAIM_STALE_SECS)
        .bind(&claim_token)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Status::internal(format!("claim node reconcile refs: {err}")))?;

        let origin_traceparents = claimed_origin_traceparents(&rows);
        let refs = rows
            .into_iter()
            .map(|row| {
                queued_facet_ref_to_proto(QueuedFacetRefRow {
                    facet: row.facet,
                    element: row.element,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(ClaimNodeReconcileRefsResponse {
            refs,
            claim_token,
            origin_traceparents,
        }))
    }

    async fn complete_node_reconcile_refs(
        &self,
        request: Request<CompleteNodeReconcileRefsRequest>,
    ) -> Result<Response<CompleteNodeReconcileRefsResponse>, Status> {
        let role_hint = optional_db_role_from_proto(request.get_ref().role)?;
        let origin = self
            .authenticated_origin_node_id(&request, &request.get_ref().node_id, role_hint)
            .await?;
        let request = request.into_inner();
        let claim_token = trimmed_required(&request.claim_token, "claim_token")?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| Status::internal(format!("begin reconcile completion: {err}")))?;

        let mut succeeded = 0_u64;
        for reference in request.succeeded {
            let reference = proto_facet_ref_to_storage(reference)?;
            let result = sqlx::query(sql!(
                "DELETE FROM node_reconcile_queue
                 WHERE node_id = $1
                   AND facet = $2
                   AND element = COALESCE($3, '')
                   AND claim_token = $4
                   AND generation = claim_generation"
            ))
            .bind(origin.id)
            .bind(&reference.facet)
            .bind(reference.element.as_deref())
            .bind(&claim_token)
            .execute(&mut *tx)
            .await
            .map_err(|err| Status::internal(format!("complete reconcile success: {err}")))?;
            succeeded = succeeded.saturating_add(result.rows_affected());
            if result.rows_affected() != 0 {
                clear_facet_apply_failed_condition(&mut tx, origin.id, &reference).await?;
            }

            sqlx::query(sql!(
                "UPDATE node_reconcile_queue
                 SET claim_token = '',
                     claim_generation = 0,
                     claimed_at = NULL,
                     next_attempt_at = NOW(),
                     updated_at = NOW()
                 WHERE node_id = $1
                   AND facet = $2
                   AND element = COALESCE($3, '')
                   AND claim_token = $4"
            ))
            .bind(origin.id)
            .bind(&reference.facet)
            .bind(reference.element.as_deref())
            .bind(&claim_token)
            .execute(&mut *tx)
            .await
            .map_err(|err| Status::internal(format!("release refreshed reconcile ref: {err}")))?;
        }

        let mut failed = 0_u64;
        for failure in request.failed {
            let Some(reference) = failure.r#ref else {
                continue;
            };
            let reference = proto_facet_ref_to_storage(reference)?;
            let error = optional_trimmed(Some(&failure.error)).unwrap_or_else(|| {
                "node-ctl reconciliation failed without an error message".to_string()
            });
            let facet_for_metrics = reference.facet.clone();
            let attempts = sqlx::query_scalar::<_, i32>(sql!(
                "SELECT attempts
                 FROM node_reconcile_queue
                 WHERE node_id = $1
                   AND facet = $2
                   AND element = COALESCE($3, '')
                   AND claim_token = $4"
            ))
            .bind(origin.id)
            .bind(&reference.facet)
            .bind(reference.element.as_deref())
            .bind(&claim_token)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| Status::internal(format!("read reconcile retry count: {err}")))?
            .unwrap_or(0);
            let next_attempt = attempts.saturating_add(1);
            let result = if next_attempt >= NODE_RECONCILE_MAX_ATTEMPTS {
                sqlx::query(sql!(
                    "UPDATE node_reconcile_queue
                     SET attempts = attempts + 1,
                         last_error = $5,
                         dead_at = NOW(),
                         dead_reason = $5,
                         claim_token = '',
                         claim_generation = 0,
                         claimed_at = NULL,
                         updated_at = NOW()
                     WHERE node_id = $1
                       AND facet = $2
                       AND element = COALESCE($3, '')
                       AND claim_token = $4
                       AND generation = claim_generation"
                ))
                .bind(origin.id)
                .bind(&reference.facet)
                .bind(reference.element.as_deref())
                .bind(&claim_token)
                .bind(&error)
                .execute(&mut *tx)
                .await
            } else {
                let delay_secs = reconcile_retry_delay_secs(attempts);
                sqlx::query(sql!(
                    "UPDATE node_reconcile_queue
                     SET attempts = attempts + 1,
                         last_error = $5,
                         claim_token = '',
                         claim_generation = 0,
                         claimed_at = NULL,
                         next_attempt_at = NOW() + ($6 * INTERVAL '1 second'),
                         updated_at = NOW()
                     WHERE node_id = $1
                       AND facet = $2
                       AND element = COALESCE($3, '')
                       AND claim_token = $4
                       AND generation = claim_generation"
                ))
                .bind(origin.id)
                .bind(&reference.facet)
                .bind(reference.element.as_deref())
                .bind(&claim_token)
                .bind(&error)
                .bind(delay_secs)
                .execute(&mut *tx)
                .await
            }
            .map_err(|err| Status::internal(format!("complete reconcile failure: {err}")))?;
            if next_attempt >= NODE_RECONCILE_MAX_ATTEMPTS && result.rows_affected() != 0 {
                pplx_space_metrics::reconcile::queue_dead_letter(&facet_for_metrics);
            }
            if result.rows_affected() != 0 {
                record_facet_apply_failed_condition(&mut tx, origin.id, &reference, &error).await?;
            }
            failed = failed.saturating_add(result.rows_affected());

            sqlx::query(sql!(
                "UPDATE node_reconcile_queue
                 SET claim_token = '',
                     claim_generation = 0,
                     claimed_at = NULL,
                     next_attempt_at = NOW(),
                     updated_at = NOW()
                 WHERE node_id = $1
                   AND facet = $2
                   AND element = COALESCE($3, '')
                   AND claim_token = $4"
            ))
            .bind(origin.id)
            .bind(&reference.facet)
            .bind(reference.element.as_deref())
            .bind(&claim_token)
            .execute(&mut *tx)
            .await
            .map_err(|err| Status::internal(format!("release refreshed failed ref: {err}")))?;
        }

        tx.commit()
            .await
            .map_err(|err| Status::internal(format!("commit reconcile completion: {err}")))?;

        Ok(Response::new(CompleteNodeReconcileRefsResponse {
            succeeded,
            failed,
        }))
    }

    async fn publish_sandbox_port_exposure_states(
        &self,
        request: Request<PublishSandboxPortExposureStatesRequest>,
    ) -> Result<Response<PublishSandboxPortExposureStatesResponse>, Status> {
        let role_hint =
            db_role_from_proto_or_default(request.get_ref().role, SpaceNodeRole::Worker)?;
        if role_hint != SpaceNodeRole::Worker && role_hint != SpaceNodeRole::Builder {
            return Err(Status::invalid_argument(
                "sandbox port exposure state can only be published by worker or builder nodes",
            ));
        }
        let origin = self
            .authenticated_origin_node_id(&request, &request.get_ref().node_id, Some(role_hint))
            .await?;
        let request = request.into_inner();
        let states = parse_port_exposure_apply_states(request.states)?;
        if states.is_empty() {
            return Ok(Response::new(PublishSandboxPortExposureStatesResponse {
                accepted: 0,
                errors: Vec::new(),
            }));
        }

        let sandbox_ids = states
            .iter()
            .map(|state| state.sandbox_id.as_uuid())
            .collect::<Vec<_>>();
        let protocols = states
            .iter()
            .map(|state| state.protocol.as_str())
            .collect::<Vec<_>>();
        let guest_ports = states
            .iter()
            .map(|state| i32::from(state.guest_port))
            .collect::<Vec<_>>();
        let apply_states = states
            .iter()
            .map(|state| state.apply_state.as_str())
            .collect::<Vec<_>>();
        let errors = states
            .iter()
            .map(|state| state.error.as_deref())
            .collect::<Vec<_>>();
        let bridge_statuses = states
            .iter()
            .map(|state| state.bridge_status.as_deref())
            .collect::<Vec<_>>();
        let service_generations = states
            .iter()
            .map(|state| state.service_generation.as_deref())
            .collect::<Vec<_>>();

        let missed = sqlx::query_as::<_, PortExposureApplyMissRow>(sql!(
            "WITH input AS (
                 SELECT *
                 FROM UNNEST(
                     $1::UUID[],
                     $2::TEXT[],
                     $3::INT[],
                     $4::TEXT[],
                     $5::TEXT[],
                     $7::TEXT[],
                     $8::TEXT[]
                 ) AS input(sandbox_id, protocol, guest_port, apply_state, error,
                            bridge_status, service_generation)
             ),
             updated AS (
                 UPDATE sandbox_port_exposures spe
                 SET apply_state = input.apply_state,
                     last_applied_node_id =
                         CASE WHEN input.apply_state = 'applied'
                              THEN $6
                              ELSE spe.last_applied_node_id
                         END,
                     last_error =
                         CASE WHEN input.apply_state = 'failed'
                              THEN input.error
                              ELSE NULL
                         END,
                     last_bridge_status =
                         COALESCE(input.bridge_status, spe.last_bridge_status),
                     last_applied_bridge_generation =
                         COALESCE(input.service_generation, spe.last_applied_bridge_generation),
                     last_observed_at = NOW(),
                     updated_at = NOW()
                 FROM input
                 JOIN sandboxes s ON s.id = input.sandbox_id
                 WHERE spe.sandbox_id = input.sandbox_id
                   AND spe.protocol = input.protocol
                   AND spe.guest_port = input.guest_port
                   AND spe.sandbox_id = s.id
                   AND s.active_node_id = $6
                 RETURNING spe.sandbox_id, spe.protocol, spe.guest_port
             )
             SELECT input.sandbox_id, input.protocol, input.guest_port
             FROM input
             LEFT JOIN updated
               ON updated.sandbox_id = input.sandbox_id
              AND updated.protocol = input.protocol
              AND updated.guest_port = input.guest_port
             WHERE updated.sandbox_id IS NULL"
        ))
        .bind(sandbox_ids)
        .bind(protocols)
        .bind(guest_ports)
        .bind(apply_states)
        .bind(errors)
        .bind(origin.id)
        .bind(bridge_statuses)
        .bind(service_generations)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Status::internal(format!("update sandbox port exposure states: {err}")))?;

        let accepted = usize_to_u64(states.len().saturating_sub(missed.len()));
        let errors = missed
            .into_iter()
            .map(|row| SandboxPortExposureApplyStateError {
                sandbox_id: row.sandbox_id.to_string(),
                protocol: port_protocol_to_proto(&row.protocol) as i32,
                guest_port: db_guest_port_to_proto(row.guest_port),
                error: "sandbox port exposure not found or not assigned to this node".to_string(),
            })
            .collect();

        Ok(Response::new(PublishSandboxPortExposureStatesResponse {
            accepted,
            errors,
        }))
    }

    async fn publish_node_state(
        &self,
        request: Request<PublishNodeStateRequest>,
    ) -> Result<Response<PublishNodeStateResponse>, Status> {
        ClusterApiService::publish_node_state(self, request).await
    }

    async fn begin_artifact_backup(
        &self,
        request: Request<BeginArtifactBackupRequest>,
    ) -> Result<Response<BeginArtifactBackupResponse>, Status> {
        ClusterApiService::begin_artifact_backup(self, request).await
    }

    async fn finalize_artifact_backup(
        &self,
        request: Request<FinalizeArtifactBackupRequest>,
    ) -> Result<Response<FinalizeArtifactBackupResponse>, Status> {
        ClusterApiService::finalize_artifact_backup(self, request).await
    }

    async fn touch_artifact_backup(
        &self,
        request: Request<TouchArtifactBackupRequest>,
    ) -> Result<Response<TouchArtifactBackupResponse>, Status> {
        ClusterApiService::touch_artifact_backup(self, request).await
    }

    async fn get_artifact_backup_location(
        &self,
        request: Request<GetArtifactBackupLocationRequest>,
    ) -> Result<Response<GetArtifactBackupLocationResponse>, Status> {
        ClusterApiService::get_artifact_backup_location(self, request).await
    }

    async fn get_quarantine_recovery_plan(
        &self,
        request: Request<GetQuarantineRecoveryPlanRequest>,
    ) -> Result<Response<GetQuarantineRecoveryPlanResponse>, Status> {
        ClusterApiService::get_quarantine_recovery_plan(self, request).await
    }

    async fn get_sandbox_snapshot_storage_uri(
        &self,
        request: Request<GetSandboxSnapshotStorageUriRequest>,
    ) -> Result<Response<GetSandboxSnapshotStorageUriResponse>, Status> {
        ClusterApiService::get_sandbox_snapshot_storage_uri(self, request).await
    }

    async fn get_suspend_snapshot_location(
        &self,
        request: Request<GetSuspendSnapshotLocationRequest>,
    ) -> Result<Response<GetSuspendSnapshotLocationResponse>, Status> {
        ClusterApiService::get_suspend_snapshot_location(self, request).await
    }
}
