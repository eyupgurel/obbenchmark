fn main() {
    prost_build::compile_protos(&["src/proto/message.proto"], &["src/proto/"]).unwrap();
}
