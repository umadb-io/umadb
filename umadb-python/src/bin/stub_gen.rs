use pyo3_stub_gen::Result;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<()> {
    let stub = umadb::stub_info()?;
    stub.generate()?;

    let pkg_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/umadb");
    let generated_dir = pkg_dir.join("umadb");
    let generated_file = generated_dir.join("__init__.pyi");
    let target_file = pkg_dir.join("umadb.pyi");

    if generated_file.exists() {
        let mut content = fs::read_to_string(&generated_file)?;
        if !content.contains("import uuid as _uuid") {
            content = format!("import uuid as _uuid\n{content}");
        }
        fs::write(&target_file, content)?;
        fs::remove_file(&generated_file)?;
        let _ = fs::remove_dir(&generated_dir);
    }

    Ok(())
}