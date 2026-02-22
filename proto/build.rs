fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor_set_path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?)
        .join("cache_descriptor_set.bin");
    println!("cargo:rerun-if-changed=cache_descriptor_set.bin");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .bytes(["."])
        .file_descriptor_set_path(&descriptor_set_path)
        .skip_protoc_run()
        .compile_protos(&["cache.proto"], &["."])?;
    Ok(())
}
