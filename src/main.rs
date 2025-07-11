use anyhow::Result;
use futures::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await.unwrap();
    tokio::spawn(async move {
        let (mut incoming_socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 1024];
        let n = incoming_socket.read(&mut buffer).await.unwrap();
        let incoming_msg = String::from_utf8_lossy(&buffer[..n]).to_string();
        println!("incoming_msg: {}", incoming_msg);
        let resp = format!("I see you! {}", incoming_msg);
        println!("resp: {:?}", resp);
        incoming_socket.write_all(resp.as_bytes()).await.unwrap();
    });
    let mut outgoing_socket = TcpStream::connect(addr).await.unwrap();
    outgoing_socket.write_all(b"Hello, world!").await.unwrap();
    let mut buffer = [0; 1024];
    let n = outgoing_socket.read(&mut buffer).await.unwrap();
    let msg: String = String::from_utf8_lossy(&buffer[..n]).to_string();
    println!("back msg: {}", msg);
}
