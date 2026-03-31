mod pipeline;

fn main() {
    if let Err(error) = pipeline::loopback::main() {
        eprintln!("[bench-matrix] error: {error:#}");
        std::process::exit(1);
    }
}
