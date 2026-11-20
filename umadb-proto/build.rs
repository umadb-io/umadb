fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = tonic_prost_build::Config::new();

    #[cfg(windows)]
    {
        config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_prost_build::configure().compile_with_config(
        config,
        &["./proto/v1/umadb.proto"],
        &["./proto/v1"],
    )?;
    Ok(())
}
