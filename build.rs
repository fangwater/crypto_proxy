fn main() {
    prost_build::Config::new()
        .compile_protos(&["period.proto"], &["."])
        .expect("failed to compile period.proto");
}
