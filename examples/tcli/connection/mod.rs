mod wsjsonrpc;

use enum_dispatch::enum_dispatch;
use uuid::Uuid;

use self::wsjsonrpc::WSJsonRPC;
use taneleer::api::*;

#[enum_dispatch]
pub trait MethodCall {
    fn create_artifact_class(&mut self, args: CreateArtifactClass);
    fn get_classes(&mut self) -> Vec<ArtifactClass>;
    fn get_artifacts(&mut self) -> Vec<Artifact>;
    fn get_sources(&mut self) -> Vec<(Uuid, Source)>;
    fn get_items(&mut self) -> Vec<ArtifactItem>;
    fn get_tags(&mut self) -> Vec<ArtifactTag>;
    fn get_usages(&mut self) -> Vec<ArtifactUsage>;
    fn get_artifact(&mut self, args: GetArtifact) -> GetArtifactRes;
    fn reserve_artifact(&mut self, args: ReserveArtifact) -> ReserveArtifactRes;
    fn commit_artifact(&mut self, args: CommitArtifact);
    fn abort_reserve(&mut self, args: AbortReserve);
    fn use_artifact(&mut self, args: UseArtifact) -> UseArtifactRes;
    fn find_last_artifact(&mut self, args: FindLastArtifact) -> FindLastArtifactRes;
    fn use_last_artifact(&mut self, args: UseLastArtifact) -> UseLastArtifactRes;

    fn close(&mut self);
}

#[enum_dispatch(MethodCall)]
pub enum Connection {
    WSJsonRPC(WSJsonRPC),
}

impl Connection {
    pub fn new() -> Self {
        Connection::WSJsonRPC(WSJsonRPC::new())
    }
}
