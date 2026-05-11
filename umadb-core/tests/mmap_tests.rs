use umadb_core::mvcc::{Mvcc, StorageOptions, ReadMethod};
use umadb_core::db::UmaDb;
use umadb_dcb::{DcbEvent, DcbEventStoreSync, DcbQuery};
use tempfile::tempdir;
use std::sync::Arc;

#[test]
fn test_mvcc_with_explicit_mmap() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("mvcc_mmap_test.db");
    
    let options = StorageOptions::default()
        .db_path(&db_path)
        .read_method(ReadMethod::Mmap)
        .page_size(4096);
    
    let mvcc = Mvcc::new(false, options).expect("mvcc new");
    let uma = UmaDb::from_arc(Arc::new(mvcc));
    
    let ev = DcbEvent::new().event_type("test").data(vec![1, 2, 3]);
    uma.append(vec![ev], None, None).expect("append");
    
    // This will use read_page which will use read_page_mmap_slice
    let response = uma.read(Some(DcbQuery::new()), None, false, None, false).expect("read");
    let events: Vec<_> = response.collect::<Result<Vec<_>, _>>().expect("collect");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.data, vec![1, 2, 3]);
}
