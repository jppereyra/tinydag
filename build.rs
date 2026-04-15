fn main() {
    tonic_build::compile_protos("proto/operator_control.proto")
        .expect("failed to compile proto/operator_control.proto");
}
