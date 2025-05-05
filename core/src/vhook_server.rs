use std::pin::Pin;
use crossbeam_channel::Sender;
use futures::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Request, Response, Status};
use mev_relayer_protos::hook_proto::{SerializedBundleResult, SubmitBundleRequest, SubmitBundleResponse, SubscribeBundleResults};
use mev_relayer_protos::hook_proto::validator_hook_server::{ValidatorHook, ValidatorHookServer};
use futures::StreamExt;
use crate::proxy::block_engine_stage::HookedBundle;

type BroadcastStream = tokio::sync::broadcast::Sender<Vec<u8>>;

#[derive(Debug)]
pub struct VHookServer {
    broadcast_stream: BroadcastStream,
    auth_code: String,
    outbound_bundle_sender: Sender<HookedBundle>
}

impl VHookServer {
    pub fn new(auth_code: String, bundle_result_broadcaster: BroadcastStream, outbound_bundle_sender: Sender<HookedBundle>) -> Self {
        Self {
            broadcast_stream: bundle_result_broadcaster,
            auth_code,
            outbound_bundle_sender
        }
    }

    pub fn to_service(self) -> ValidatorHookServer<Self> {
        ValidatorHookServer::new(self)
    }
}

#[async_trait]
impl ValidatorHook for VHookServer {
    async fn submit_bundle(&self, request: Request<SubmitBundleRequest>) -> Result<Response<SubmitBundleResponse>, Status> {
        let bundle = request.into_inner().bundle;

        let bundle = match bincode::deserialize::<HookedBundle>(&bundle) {
            Ok(bundle) => bundle,
            Err(_) => return Err(Status::invalid_argument("Invalid bundle"))
        };

        Ok(Response::new(SubmitBundleResponse {
            success: self.outbound_bundle_sender.try_send(bundle).is_ok()
        }))
    }

    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<SerializedBundleResult, Status>> + Send>>;

    async fn subscribe(&self, request: Request<SubscribeBundleResults>) -> Result<Response<Self::SubscribeStream>, Status> {
        if request.get_ref().auth_code != self.auth_code {
            return Err(Status::unauthenticated("Invalid auth code"));
        }

        let mut broadcast_stream = self.broadcast_stream.subscribe();

        let (tx, rx) = tokio::sync::mpsc::channel(u16::MAX as usize);

        tokio::spawn(async move {
            while let Ok(message) = broadcast_stream.recv().await {
                if tx
                    .send(SerializedBundleResult { content: message })
                    .await
                    .is_err()
                {
                    // client has disconnected, stop this task by exiting the loop
                    break;
                }
            }
        });

        let stream = ReceiverStream::new(rx).map(Ok);
        Ok(Response::new(Box::pin(stream)))
    }
}
