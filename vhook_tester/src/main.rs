use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::Instant;
use serde::Serialize;
use solana_sdk::transaction::VersionedTransaction;

#[derive(Serialize)]
struct HookedBundle {
    uuid: String,
    auth_code: String,
    transactions: Vec<Vec<u8>>
}

const AMOUNT: usize = 10000;

fn main() -> anyhow::Result<()> {
    let mut socket = TcpStream::connect("127.0.0.1:5105")?;
    socket.set_nodelay(true)?;

    let dest: SocketAddr = "127.0.0.1:5105".parse()?;


    let vtx = VersionedTransaction::default();
    let txs = vec![bincode::serialize(&vtx)?];

    let bundle = HookedBundle { uuid: "hello_test_uuid_here".to_string(), auth_code: "hi".to_string(), transactions: txs };

    println!("sending packet");

    let bytes = bincode::serialize(&bundle)?;
    let len = bytes.len() as u16;
    let len_bytes = len.to_le_bytes();

    println!("size: {}", bytes.len());

    let start = Instant::now();
    for _ in 0..AMOUNT {
        socket.write_all(&[len_bytes.as_slice(), &bytes].concat())?;
    }
    let end = Instant::now();


    println!("{} pkts sent in {:?}", AMOUNT, end.duration_since(start));

    thread::sleep(std::time::Duration::from_secs(5));

    Ok(())
}
