use futures::Stream as FutureStream;
use futures::StreamExt;
use futures::prelude::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

pub async fn start_server() {
    println!("start server");
    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 1024];
        let n = socket.read(&mut buffer).await.unwrap();
        let msg: String = String::from_utf8_lossy(&buffer[..n]).to_string();
        println!("msg: {}", msg);
        let resp = format!("msg received: {}", msg);
        socket.write_all(resp.as_bytes()).await.unwrap();
        // println!("write done");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[tokio::test]
    async fn base_socket_should_work() {
        tokio::spawn(start_server());
        sleep(Duration::from_millis(100)).await;
        println!("start client");
        let addr = "127.0.0.1:8080";
        println!("123");
        let mut outgoing_socket = TcpStream::connect(addr).await.unwrap();
        println!("connection established");
        let msg = "Hello, world!";
        outgoing_socket.write_all(msg.as_bytes()).await.unwrap();
        let mut buffer = [0; 1024];
        let n = outgoing_socket.read(&mut buffer).await.unwrap();
        let resp: String = String::from_utf8_lossy(&buffer[..n]).to_string();
        println!("back msg: {}", resp);
        let est_resp = format!("msg received: {}", msg);
        assert_eq!(resp, est_resp);
    }
}
