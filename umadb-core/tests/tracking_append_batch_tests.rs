use serial_test::serial;
use tempfile::tempdir;
use umadb_core::db::UmaDB;
use umadb_dcb::{DCBAppendCondition, DCBEvent, DCBEventStoreSync, TrackingInfo};

#[test]
#[serial]
fn append_batch_with_per_item_tracking_enforces_monotonicity() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("tracking-batch.db");
    let store = UmaDB::new(&db_path).unwrap();

    // simple helper to make an event
    let mk = |t: &str| DCBEvent {
        event_type: t.to_string(),
        data: b"x".to_vec(),
        tags: vec!["t".into()],
        uuid: None,
    };

    // Batch with three items on same source "S":
    // 1) create tracking at pos 5 (should succeed and record 5)
    // 2) try same pos 5 again (should IntegrityError and not append)
    // 3) greater pos 6 (should succeed and update to 6)
    let items = vec![
        (
            vec![mk("A1")],
            None::<DCBAppendCondition>,
            Some(TrackingInfo {
                source: "S".into(),
                position: 5,
            }),
        ),
        (
            vec![mk("A2")],
            None,
            Some(TrackingInfo {
                source: "S".into(),
                position: 5,
            }),
        ),
        (
            vec![mk("A3")],
            None,
            Some(TrackingInfo {
                source: "S".into(),
                position: 6,
            }),
        ),
    ];

    let results = store.append_batch(items).unwrap();
    assert_eq!(3, results.len());

    // 1) Ok(last = 1)
    match &results[0] {
        Ok(p) => assert_eq!(1, *p),
        other => panic!("unexpected: {:?}", other),
    }
    // 2) Err(IntegrityError)
    match &results[1] {
        Err(e) => assert!(matches!(e, umadb_dcb::DCBError::IntegrityError(_))),
        other => panic!("expected IntegrityError, got {:?}", other),
    }
    // 3) Ok(last = 2)
    match &results[2] {
        Ok(p) => assert_eq!(2, *p),
        other => panic!("unexpected: {:?}", other),
    }

    // Verify recorded tracking position is 6
    assert_eq!(Some(6), store.get_tracking_info("S").unwrap());

    // Also verify head is 2 (second failed append did not advance)
    assert_eq!(Some(2), store.head().unwrap());
}
