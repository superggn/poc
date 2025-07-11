use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub struct MyStruct {
    conn: TcpStream,
}

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

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    // 用 tokio::test 可以有 async fn
    #[tokio::test]
    async fn it_works_2() {
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
