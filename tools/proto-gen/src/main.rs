//! Native-only generation from Buf's descriptor set; no external protoc needed.
use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: pintle-proto-gen DESCRIPTOR_SET OUTPUT_DIRECTORY".into());
    }
    let descriptors = prost_types::FileDescriptorSet::decode(std::fs::read(&args[1])?.as_slice())?;
    std::fs::create_dir_all(&args[2])?;
    prost_build::Config::new()
        .boxed(".pintle.v1.AcquireResponse.outcome.acquired")
        .out_dir(&args[2])
        .compile_fds(descriptors)?;
    Ok(())
}
