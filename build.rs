fn main() {
    tonic_prost_build::compile_protos("protos/raft_service.proto")
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
