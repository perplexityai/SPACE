use super::*;
use crate::node_publish::store;
use crate::node_publish::validate::ValidatedPublish;
use crate::node_publish::validate::node_role::PublicationRoleIntent;

impl ClusterApiService {
    pub(super) async fn publish_node_state(
        &self,
        request: Request<PublishNodeStateRequest>,
    ) -> Result<Response<PublishNodeStateResponse>, Status> {
        let role_intent = PublicationRoleIntent::try_from_proto(request.get_ref())?;
        let origin = self
            .authenticated_origin_node_id(
                &request,
                &request.get_ref().node_id,
                role_intent.role_hint(),
            )
            .await?;
        role_intent.validate_authenticated_role(origin.role)?;
        let publish = ValidatedPublish::try_from_proto(origin, request.into_inner())?;
        let outcome = store::apply(&self.pool, publish).await?;
        Ok(Response::new(outcome.emit_post_commit()))
    }
}
