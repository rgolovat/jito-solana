use crate::packet_bundle::PacketBundle;
use crate::proxy::block_engine_stage::HookedBundle;
use crossbeam_channel::Sender;
use futures::Stream;
use futures::StreamExt;
use mev_relayer_protos::hook_proto::validator_hook_server::{ValidatorHook, ValidatorHookServer};
use mev_relayer_protos::hook_proto::{
    AuthCodeRequest, SerializedBundle, SerializedBundleResult, SubmitBundleRequest,
    SubmitBundleResponse,
};
use solana_sdk::transaction::VersionedTransaction;
use std::pin::Pin;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Request, Response, Status};

type BroadcastStream = tokio::sync::broadcast::Sender<Vec<u8>>;

#[derive(Debug)]
pub struct VHookServer {
    bundle_results_bd: BroadcastStream,
    outgoing_bundle_bd: tokio::sync::broadcast::Sender<Vec<PacketBundle>>,
    auth_code: String,
    outbound_bundle_sender: Sender<HookedBundle>,
}

impl VHookServer {
    pub fn new(
        auth_code: String,
        incoming_bundle_bd: BroadcastStream,
        outgoing_bundle_bd: tokio::sync::broadcast::Sender<Vec<PacketBundle>>,
        outbound_bundle_sender: Sender<HookedBundle>,
    ) -> Self {
        Self {
            bundle_results_bd: incoming_bundle_bd,
            outgoing_bundle_bd,
            auth_code,
            outbound_bundle_sender,
        }
    }

    pub fn to_service(self) -> ValidatorHookServer<Self> {
        ValidatorHookServer::new(self)
    }
}

#[async_trait]
impl ValidatorHook for VHookServer {
    async fn submit_bundle(
        &self,
        request: Request<SubmitBundleRequest>,
    ) -> Result<Response<SubmitBundleResponse>, Status> {
        let bundle = request.into_inner().bundle;

        let bundle = match bincode::deserialize::<HookedBundle>(&bundle) {
            Ok(bundle) => bundle,
            Err(_) => return Err(Status::invalid_argument("Invalid bundle")),
        };

        Ok(Response::new(SubmitBundleResponse {
            success: self.outbound_bundle_sender.try_send(bundle).is_ok(),
        }))
    }

    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<SerializedBundleResult, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<AuthCodeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        if request.get_ref().auth_code != self.auth_code {
            return Err(Status::unauthenticated("Invalid auth code"));
        }

        let mut broadcast_stream = self.bundle_results_bd.subscribe();

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

    type SubscribeJitoBundleStream =
        Pin<Box<dyn Stream<Item = Result<SerializedBundle, Status>> + Send>>;
    async fn subscribe_jito_bundle(
        &self,
        request: Request<AuthCodeRequest>,
    ) -> Result<Response<Self::SubscribeJitoBundleStream>, Status> {
        if request.get_ref().auth_code != self.auth_code {
            return Err(Status::unauthenticated("Invalid auth code"));
        }

        let mut broadcast_stream = self.outgoing_bundle_bd.subscribe();

        let (tx, rx) = tokio::sync::mpsc::channel(u16::MAX as usize);

        tokio::spawn(async move {
            while let Ok(message) = broadcast_stream.recv().await {
                'main: for pkt_bundle in message {
                    let bundle_id = pkt_bundle.bundle_id;
                    let mut vtxs: Vec<VersionedTransaction> =
                        Vec::with_capacity(pkt_bundle.batch.len());

                    for pkt in pkt_bundle.batch.into_iter() {
                        match pkt.deserialize_slice(..) {
                            Ok(vtx) => vtxs.push(vtx),
                            Err(e) => {
                                error!("Failed to deserialize packet: {:?}", e);
                                continue 'main;
                            }
                        };
                    }

                    // send serialized &[VersionedTransaction]
                    if tx
                        .send(SerializedBundle {
                            content: bincode::serialize(&mev_relayer_protos::vhook::JitoBundle {
                                bundle_id: bundle_id.clone(),
                                transactions: vtxs,
                            })
                            .unwrap(),
                        })
                        .await
                        .is_err()
                    {
                        // client has disconnected, stop this task by exiting the loop
                        break;
                    }
                }
            }
        });

        let stream = ReceiverStream::new(rx).map(Ok);
        Ok(Response::new(Box::pin(stream)))
    }
}
