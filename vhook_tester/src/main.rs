use std::thread;
use laminar::Packet;
use serde::Serialize;
use solana_sdk::transaction::VersionedTransaction;

#[derive(Serialize)]
struct HookedBundle {
    uuid: String,
    auth_code: String,
    transactions: Vec<Vec<u8>>
}

fn main() -> anyhow::Result<()> {
    let mut socket = laminar::Socket::bind("0.0.0.0:0")?;


    let dest = "127.0.0.1:5105".parse()?;


    let packet_sender = socket.get_packet_sender();

    // Starts the socket, which will start a poll mechanism to receive and send messages.
    let _thread = thread::spawn(move || socket.start_polling());


    let vtx = VersionedTransaction::default();
    let txs = vec![bincode::serialize(&vtx)?];

    let bundle = HookedBundle { uuid: "hello_test_uuid_here".to_string(), auth_code: "hi".to_string(), transactions: txs };

    println!("sending packet");

    let bytes = bincode::serialize(&bundle)?;

    println!("size: {}", bytes.len());

    packet_sender.send(Packet::reliable_unordered(dest, bytes))?;

    println!("packet sent, waiting 5sec to flush");

    thread::sleep(std::time::Duration::from_secs(5));

    Ok(())
}
