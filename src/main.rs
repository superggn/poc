#[tokio::main]
async fn main() {
    // let addr = "127.0.0.1:8080";
    // let listener = TcpListener::bind(addr).await.unwrap();
    // tokio::spawn(async move {
    //     let (mut incoming_socket, _) = listener.accept().await.unwrap();
    //     let mut buffer = [0; 1024];
    //     let n = incoming_socket.read(&mut buffer).await.unwrap();
    //     let incoming_msg = String::from_utf8_lossy(&buffer[..n]).to_string();
    //     println!("incoming_msg: {}", incoming_msg);
    //     let resp = format!("I see you! {}", incoming_msg);
    //     println!("resp: {:?}", resp);
    //     incoming_socket.write_all(resp.as_bytes()).await.unwrap();
    // });
    // let mut outgoing_socket = TcpStream::connect(addr).await.unwrap();
    // outgoing_socket.write_all(b"Hello, world!").await.unwrap();
    // let mut buffer = [0; 1024];
    // let n = outgoing_socket.read(&mut buffer).await.unwrap();
    // let msg: String = String::from_utf8_lossy(&buffer[..n]).to_string();
    // println!("back msg: {}", msg);
    let fut1 = futures::future::ready(42);
    let fut2 = async {
        println!("fut2");
        42
    };
    let fut3 = {
        println!("fut3");
        futures::future::ready(42)
    };
    let res1 = fut1.await;
    let res2 = fut2.await;
    let res3 = fut3.await;
    println!("res1: {:?}", res1);
    println!("res2: {:?}", res2);
    println!("res3: {:?}", res3);
}
