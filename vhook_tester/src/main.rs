use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::{Duration, Instant};
use serde::Serialize;
use tonic::codec::CompressionEncoding;
use mev_relayer_protos::hook_proto::{SubmitBundleRequest, SubscribeBundleResults};
use mev_relayer_protos::hook_proto::validator_hook_client::ValidatorHookClient;
use mev_relayer_protos::vhook::VHookBundleStatus;
use solana_client::rpc_client::SerializableTransaction;
use solana_sdk::message::{v0, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{EncodableKey, Keypair, Signer};
use solana_sdk::transaction::VersionedTransaction;

#[derive(Serialize)]
struct HookedBundle {
    uuid: String,
    auth_code: String,
    transactions: Vec<Vec<u8>>
}

const AMOUNT: usize = 1000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // let dest = "185.189.45.80:5105";
    let dest = "http://127.0.0.1:5105";
    let rpc_client = solana_client::nonblocking::rpc_client::RpcClient::new("http://127.0.0.1:8899".to_string());


    let mut client = ValidatorHookClient::connect(dest).await?;

    println!("Connected to validator hook({})", dest);

    // subscribe to bundle results
    let mut sub = client.subscribe(SubscribeBundleResults {
        auth_code: ":)".to_string()
    }).await?.into_inner();

    tokio::spawn(async move {
        while let Ok(Some(msg)) = sub.message().await {
            let bundle_result: VHookBundleStatus = bincode::deserialize(&msg.content).unwrap();
            println!("Received bundle_result {:?}", bundle_result);
        }
    });


    let my_kp = Keypair::read_from_file("keypair.json").unwrap();
    let recipient = Keypair::new().pubkey();

    let transfer = solana_sdk::system_instruction::transfer(
        &my_kp.pubkey(),
        &recipient,
        10_000_000
    );

    let recent_hash = rpc_client.get_latest_blockhash().await?;

    let bad_vtx = VersionedTransaction::try_new(
        VersionedMessage::V0(v0::Message::try_compile(
            &my_kp.pubkey(),
            &[transfer],
            &[],
            recent_hash
        )?),
        &[my_kp]
    )?;
    println!("signature: {}, accs: {:?}", bad_vtx.get_signature(), bad_vtx.message.static_account_keys());

    let bad_vtx = bincode::serialize(&bad_vtx)?;

    // send a bad bundle
    let bad_bundle = HookedBundle {
        uuid: "bad_bundle".to_string(),
        auth_code: ":)".to_string(),
        transactions: vec![bad_vtx],
    };
    let bad_bundle = bincode::serialize(&bad_bundle)?;

    println!("sending bad bundle");

    let ok = client.submit_bundle(SubmitBundleRequest {
        bundle: bad_bundle,
    }).await?.into_inner();

    println!("Submitted bundle {:?}", ok);


    tokio::time::sleep(Duration::MAX).await;
    Ok(())
}
