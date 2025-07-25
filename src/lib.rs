use anyhow::Result;
use futures::prelude::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};
use tokio_util::{
    codec::{BytesCodec, Framed, LinesCodec},
    compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt},
};
use yamux::{Config, Connection, Mode, Stream};

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

pub async fn handle_plain_stream<S>(prefix: String, stream: S)
where
    S: AsyncRead + AsyncWrite,
{
    use futures::pin_mut;
    pin_mut!(stream);
    let mut framed = Framed::new(stream, BytesCodec::new());
    // pin_mut!(framed); // 👈 关键在这

    while let Some(Ok(buf)) = framed.next().await {
        println!("Got: {:?}", buf);
        let s = String::from_utf8_lossy(&buf);
        println!("decoded msg: {}", s);
        let response = format!("{}: {}", prefix, s);
        // let resp = response.as_bytes();
        use bytes::Bytes;
        let resp = Bytes::copy_from_slice(response.as_bytes());
        framed.send(resp).await.unwrap();
    }
}

pub async fn start_yamux_server() {
    println!("start yamux server");
    let addr = "127.0.0.1:8001";
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        println!("start waiting incoming!");
        let (tokio_socket, _) = listener.accept().await.unwrap();
        println!("conn established!");
        let config = Config::default();
        // largest frame size? => todo confirm meaning
        // config.set_split_send_size(4 * 1024);
        let mut conn = Connection::new(tokio_socket.compat(), config, Mode::Server);
        println!("10");
        let mut incoming_poll_fn = stream::poll_fn(move |cx| {
            println!("[poll_next_inbound] polled");
            conn.poll_next_inbound(cx)
        });
        println!("11");
        let new_stream = incoming_poll_fn.next().await;
        println!("new_stream: {:?}", new_stream);
        match new_stream {
            Some(Ok(stream)) => {
                println!("21");
                tokio::spawn(async move {
                    incoming_poll_fn
                        .for_each(|maybe_stream| {
                            tokio::spawn(handle_plain_stream(
                                "".to_string(),
                                maybe_stream.unwrap().compat(),
                            ));
                            // drop(maybe_stream);
                            future::ready(())
                        })
                        .await;
                });
                process_client(stream).await;
                // let fut = handle_plain_stream("".to_string(), stream.compat());
                // tokio::spawn(fut);
                // handle_plain_stream("".to_string(), stream.compat()).await;
                // process_client(stream).await;
            }
            Some(Err(e)) => {
                println!("error here: {:?}", e);
                // handle_error(e);
            }
            None => {
                println!("22");
                // Handle None case if needed
            }
        }
    }
}

async fn process_client(stream: yamux::Stream) {
    let mut framed = Framed::new(stream.compat(), LinesCodec::new());

    while let Some(Ok(line)) = framed.next().await {
        println!("Got: {}", line);
        framed
            .send(format!("Hello! I got '{}'", line))
            .await
            .unwrap();
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    // 用 tokio::test 可以有 async fn
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

    #[tokio::test]
    async fn basic_yamux_should_work() {
        // closure => async yamux server
        // start this async yamux server
        println!("start yamux now!");
        tokio::spawn(async { start_yamux_server().await });
        sleep(Duration::from_millis(1000)).await; // Wait for server to be ready

        // Connect to Yamux server over TCP
        println!("1");
        let tcp_stream = TcpStream::connect("127.0.0.1:8001").await.unwrap();
        println!("2");
        let mut config = Config::default();
        // config.set_split_send_size(4 * 1024);
        let yamux_conn = Connection::new(tcp_stream.compat(), config, Mode::Client);

        tokio::pin!(yamux_conn); // pin for poll_next_stream etc.
        println!("3");

        // poll 所有 stream 下的数据
        // 这里用的是 future::poll_fn, 正常 poll_new_outbound(cx)
        // 所以我那边的问题不是拿不到 yamuxstream, 而是拿到的 stream 不对
        // client 这边新开一个 substream， 后面处理下， 继续弄别的
        let substream = future::poll_fn(|cx| yamux_conn.poll_new_outbound(cx))
            .await
            .unwrap();
        println!("4");

        let mut framed_handle = Framed::new(substream.compat(), BytesCodec::new());
        let msg: bytes::Bytes = "hello, yamux".into();
        println!("5");
        framed_handle.send(msg).await.unwrap();
        println!("msg sent!");
        println!("6");
        let resp = framed_handle.next().await.unwrap().unwrap();
        println!("7");
        let plain_resp = String::from_utf8_lossy(&resp);
        println!("resp: {}", plain_resp);

        ()
    }
}
