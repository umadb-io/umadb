fn main() -> Result<(), Box<dyn std::error::Error>> {
    // unsafe {
    //     std::env::set_var("PROTOC", protobuf_src::protoc());
    // }
    tonic_prost_build::compile_protos("./umadb.proto")?;
    Ok(())
}
