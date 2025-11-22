// cargo bench --bench mvcc_commit_flame --features flamegraphs

use std::fs;
use std::path::Path;
use umadb_core::db::unconditional_append;
use umadb_dcb::DCBEvent;
use uuid::Uuid;

fn main() -> std::io::Result<()> {
    use pprof::ProfilerGuard;
    use std::fs::File;
    use std::time::{Duration, Instant};
    // use tempfile::tempdir;
    use umadb_benches::bench_api::BenchDb;

    fn fresh_db(page_size: usize) -> BenchDb {
        let large_db_path = "../../umadb-throughput.db-60M";
        // let dir = tempdir().expect("tempdir");
        // let db_path = dir.path().join("umadb.commit.flame");
        let db_path = "../../commit-flame-copy-of-umadb-throughput.db-60M";
        println!("Copying large DB from {} to {}", large_db_path, db_path);
        match fs::copy(large_db_path, db_path) {
            Ok(bytes) => println!("Copied {} bytes", bytes),
            Err(e) => panic!("Failed to copy file: {e}"),
        }
        println!("Copied large DB from: {}", large_db_path);
        // BenchDb::new(&db_path, page_size).expect("BenchDb::new")
        BenchDb::new(Path::new(db_path), page_size).expect("BenchDb::new")
    }

    fn profile_to_svg<F>(name: &str, mut work: F) -> std::io::Result<()>
    where
        F: FnMut(),
    {
        let guard = ProfilerGuard::new(100).expect("ProfilerGuard");
        let deadline = Instant::now() + Duration::from_millis(300);
        while Instant::now() < deadline {
            work();
        }

        if let Ok(report) = guard.report().build() {
            let out_dir = std::path::Path::new("../target/flamegraphs");
            std::fs::create_dir_all(out_dir)?;
            let path = out_dir.join(format!("{name}.svg"));
            let mut opts = pprof::flamegraph::Options::default();
            let file = File::create(&path)?;
            report
                .flamegraph_with_options(file, &mut opts)
                .expect("write flamegraph");
            println!("✅ Wrote {}", path.canonicalize()?.display());
        }
        Ok(())
    }

    fn generate_flamegraphs() -> std::io::Result<()> {
        let page_size = 4096usize;
        let db = fresh_db(page_size);

        for warmup_i in 0..10000 {
            let start = Instant::now();
            for _ in 0..1000 {
                let mut w = db.writer();
                let event = DCBEvent {
                    event_type: "batch-type".to_string(),
                    data: "batch-data".to_string().into_bytes(),
                    tags: vec![format!("tag-{}", Uuid::new_v4())],
                    uuid: None,
                };

                unconditional_append(&db.mvcc, &mut w, vec![event]).expect("Failed to append");
                // println!("Warm up {}: Dirty pages: {}", warmup_i, &w.dirty.len());
                db.mvcc.commit(&mut w).expect("Failed to commit");
            }
            let elapsed = start.elapsed();
            // Calculate rate (events per second)
            let rate = 1000.0 / elapsed.as_secs_f64();
            println!("Warm up {}: Rate: {} events/s", warmup_i, rate);
            if rate < 100.0 {
                break;
            }
        }

        profile_to_svg(&format!("append_at_large_volume"), || {
            let start = Instant::now();

            for _ in 0..1000 {
                let mut w = db.writer();
                let event = DCBEvent {
                    event_type: "batch-type".to_string(),
                    data: "batch-data".to_string().into_bytes(),
                    tags: vec![format!("tag-{}", Uuid::new_v4())],
                    uuid: None,
                };

                unconditional_append(&db.mvcc, &mut w, vec![event]).expect("Failed to append");
                // println!("Dirty pages: {}", &w.dirty.len());
                db.mvcc.commit(&mut w).expect("Failed to commit");
            }
            let elapsed = start.elapsed();
            // Calculate rate (events per second)
            let rate = 1000.0 / elapsed.as_secs_f64();

            // Log the measurement: sequence_position rate
            println!("Rate: {} events/s", rate);
        })?;

        Ok(())
    }

    generate_flamegraphs()
}
