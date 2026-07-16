use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=proto/replication.proto");
    let descriptor_path = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"))
        .join("klights_replication_descriptor.bin");
    tonic_prost_build::configure()
        .build_transport(false)
        .file_descriptor_set_path(&descriptor_path)
        .type_attribute(
            "klights.replication.LeaderMessage.payload",
            "#[allow(clippy::large_enum_variant)]",
        )
        .compile_protos(&["proto/replication.proto"], &["proto"])
        .expect("failed to compile replication gRPC protobuf");
}
