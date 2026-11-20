use std::net::TcpListener;

pub fn allocate_grpc_addr() -> (String, String) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("resolve local addr");
    drop(listener);
    (format!("{addr}"), format!("http://{addr}"))
}