use pyo3_stub_gen::Result;

fn main() -> Result<()> {
    // `stub_info` is a function defined by `define_stub_info_gatherer!` macro.
    let stub = umadb::stub_info()?;
    stub.generate()?;

    // Add import uuid to the generated file
    let path = "umadb-python/python/umadb/__init__.pyi";
    if let Ok(content) = std::fs::read_to_string(path) {
        if !content.contains("import uuid as _uuid") {
            std::fs::write(path, format!("import uuid as _uuid\n{}", content))?;
        }
    }
    Ok(())
}
