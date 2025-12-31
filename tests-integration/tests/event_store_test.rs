use std::net::TcpListener;
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use umadb_client::UmaDBClient;
use umadb_core::db::UmaDB;
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreSync, DCBQuery, DCBQueryItem,
};
use umadb_server::start_server;
use uuid::Uuid;

// Helper function to run the test with implementations of the DCBEventStoreSync trait
pub fn dcb_event_store_test<T: DCBEventStoreSync>(event_store: &T) {
    // Test head() method on empty store
    let head_position = event_store.head().unwrap();
    assert_eq!(None, head_position);

    // Read all, expect no results.
    let (result, head) = event_store.read_with_head(None, None, false, None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(None, head);

    // Read all backwards, expect no results.
    let (result, head) = event_store.read_with_head(None, None, true, None).unwrap();
    assert_eq!(0, result.len());
    assert_eq!(None, head);

    // Append one event.
    let event1 = DCBEvent {
        event_type: "type1".to_string(),
        data: b"data1".to_vec(),
        tags: vec!["tagX".to_string()],
        uuid: None,
    };
    let position = event_store
        .append(vec![event1.clone()], None, None)
        .unwrap();

    // Check the returned position is 1.
    assert_eq!(1, position);

    // Test head() method after appending one event
    let head_position = event_store.head().unwrap();
    assert_eq!(Some(1), head_position);

    // Read all, expect one event.
    let (result, head) = event_store.read_with_head(None, None, false, None).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(event1.uuid, result[0].event.uuid);
    assert_eq!(Some(1), head);

    // Read all backwards, expect one event.
    let (result, head) = event_store.read_with_head(None, None, true, None).unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(event1.uuid, result[0].event.uuid);
    assert_eq!(Some(1), head);

    // Read all from position 2, expect no events.
    let (result, head) = event_store
        .read_with_head(None, Some(2), false, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Read all backwards from position 2, expect one event.
    let (result, head) = event_store
        .read_with_head(None, Some(2), true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(1), head);

    // Read all backwards from position 1, expect one event.
    let (result, head) = event_store
        .read_with_head(None, Some(1), true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(1), head);

    // Read all backwards from position 0, expect no events.
    let (result, head) = event_store
        .read_with_head(None, Some(0), true, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Read all limit 1, expect one event.
    let (result, head) = event_store
        .read_with_head(None, None, false, Some(1))
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Read all backwards limit 1, expect one event.
    let (result, head) = event_store
        .read_with_head(None, None, true, Some(1))
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Read all limit 0, expect no events (and head is None).
    let (result, head) = event_store
        .read_with_head(None, None, false, Some(0))
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(None, head);

    // Read all backwards limit 0, expect no events (and head is None).
    let (result, head) = event_store
        .read_with_head(None, None, true, Some(0))
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(None, head);

    // Read events with type1, expect 1 event.
    let query_type1 = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type1".to_string()],
            tags: vec![],
        }],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_type1.clone()), None, false, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Read events with type1 backwards, expect 1 event.
    let (result, head) = event_store
        .read_with_head(Some(query_type1.clone()), None, true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Read events with type2, expect no events.
    let query_type2 = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type2".to_string()],
            tags: vec![],
        }],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_type2.clone()), None, false, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Read events with type2 backwards, expect no events.
    let (result, head) = event_store
        .read_with_head(Some(query_type2.clone()), None, true, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Read events with tagX, expect one event.
    let query_tag_x = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec![],
            tags: vec!["tagX".to_string()],
        }],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_tag_x.clone()), None, false, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Backwards
    let (result, head) = event_store
        .read_with_head(Some(query_tag_x.clone()), None, true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Read events with tagY, expect no events.
    let query_tag_y = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec![],
            tags: vec!["tagY".to_string()],
        }],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_tag_y.clone()), None, false, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    let (result, head) = event_store
        .read_with_head(Some(query_tag_y.clone()), None, true, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Read events with type1 and tagX, expect one event.
    let query_type1_tag_x = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type1".to_string()],
            tags: vec!["tagX".to_string()],
        }],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_type1_tag_x.clone()), None, false, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(1), head);

    // Backwards
    let (result, head) = event_store
        .read_with_head(Some(query_type1_tag_x.clone()), None, true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(1), head);

    // Read events with type1 and tagY, expect no events.
    let query_type1_tag_y = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type1".to_string()],
            tags: vec!["tagY".to_string()],
        }],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_type1_tag_y.clone()), None, false, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    let (result, head) = event_store
        .read_with_head(Some(query_type1_tag_y.clone()), None, true, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Read events with type2 and tagX, expect no events.
    let query_type2_tag_x = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type2".to_string()],
            tags: vec!["tagX".to_string()],
        }],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_type2_tag_x.clone()), None, false, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    let (result, head) = event_store
        .read_with_head(Some(query_type2_tag_x.clone()), None, true, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(1), head);

    // Append two more events.
    let event2 = DCBEvent {
        event_type: "type2".to_string(),
        data: b"data2".to_vec(),
        tags: vec!["tagA".to_string(), "tagB".to_string()],
        uuid: None,
    };
    let event3 = DCBEvent {
        event_type: "type3".to_string(),
        data: b"data3".to_vec(),
        tags: vec!["tagA".to_string(), "tagC".to_string()],
        uuid: None,
    };
    let position = event_store
        .append(vec![event2.clone(), event3.clone()], None, None)
        .unwrap();

    // Check the returned position is 3
    assert_eq!(3, position);

    // Test head() method after appending more events
    let head_position = event_store.head().unwrap();
    assert_eq!(Some(3), head_position);

    // Read all, expect 3 events (in ascending order).
    let (result, head) = event_store.read_with_head(None, None, false, None).unwrap();
    assert_eq!(3, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(event2.data, result[1].event.data);
    assert_eq!(event3.data, result[2].event.data);
    assert_eq!(Some(3), head);

    // Read all backwards, expect 3 events (in descending order).
    let (result, head) = event_store.read_with_head(None, None, true, None).unwrap();
    assert_eq!(3, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(event2.data, result[1].event.data);
    assert_eq!(event1.data, result[2].event.data);
    assert_eq!(Some(3), head);

    // Read all start 2, expect two events.
    let (result, head) = event_store
        .read_with_head(None, Some(2), false, None)
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event3.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Read all backwards start 2, expect two events.
    let (result, head) = event_store
        .read_with_head(None, Some(2), true, None)
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event1.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Read all start 3, expect one event.
    let (result, head) = event_store
        .read_with_head(None, Some(3), false, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read all backwards start 3, expect three events.
    let (result, head) = event_store
        .read_with_head(None, Some(3), true, None)
        .unwrap();
    assert_eq!(3, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(event2.data, result[1].event.data);
    assert_eq!(event1.data, result[2].event.data);
    assert_eq!(Some(3), head);

    // Read all start 2, limit 1, expect one event.
    let (result, head) = event_store
        .read_with_head(None, Some(2), false, Some(1))
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(Some(2), head);

    // Read all start 2, limit 1, expect one event.
    let (result, head) = event_store
        .read_with_head(None, Some(2), true, Some(1))
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(Some(2), head);

    // Read all start 11, limit 10, expect zero events.
    let (result, head) = event_store
        .read_with_head(None, Some(11), false, Some(10))
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(None, head);

    // Read all start 11, limit 10, expect three events.
    let (result, head) = event_store
        .read_with_head(None, Some(11), true, Some(10))
        .unwrap();
    assert_eq!(3, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(event2.data, result[1].event.data);
    assert_eq!(event1.data, result[2].event.data);
    assert_eq!(Some(1), head); // TODO: Maybe should return highest rather than lowest?

    // Read type1 start 2, expect no events.
    let (result, head) = event_store
        .read_with_head(Some(query_type1.clone()), Some(2), false, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(3), head);

    // Read type1 backwards start 2, expect one event.
    let (result, head) = event_store
        .read_with_head(Some(query_type1.clone()), Some(2), true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read tagX start 2, expect no events.
    let (result, head) = event_store
        .read_with_head(Some(query_tag_x.clone()), Some(2), false, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(3), head);

    // Read tagX backwards start 2, expect no events.
    let (result, head) = event_store
        .read_with_head(Some(query_tag_x.clone()), Some(2), true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read tagX start 2, limit 1 expect no events.
    let (result, head) = event_store
        .read_with_head(Some(query_tag_x.clone()), Some(2), false, Some(1))
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(None, head);

    // Read tagX backwards start 2, limit 1 expect one event.
    let (result, head) = event_store
        .read_with_head(Some(query_tag_x.clone()), Some(2), true, Some(1))
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(1), head);

    // Read type1 and tagX start 2, expect no events.
    let (result, head) = event_store
        .read_with_head(Some(query_type1_tag_x.clone()), Some(2), false, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(3), head);

    // Read type1 and tagX backwards start 2, expect no events.
    let (result, head) = event_store
        .read_with_head(Some(query_type1_tag_x.clone()), Some(2), true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read events with tagA, expect two events.
    let query_tag_a = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec![],
            tags: vec!["tagA".to_string()],
        }],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_tag_a.clone()), None, false, None)
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event3.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Backwards
    let (result, head) = event_store
        .read_with_head(Some(query_tag_a.clone()), None, true, None)
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(event2.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Read events with tagA and tagB, expect one event.
    let query_tag_a_and_b = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec![],
            tags: vec!["tagA".to_string(), "tagB".to_string()],
        }],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_tag_a_and_b.clone()), None, false, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Backwards
    let (result, head) = event_store
        .read_with_head(Some(query_tag_a_and_b.clone()), None, true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read events with tagB or tagC, expect two events.
    let query_tag_b_or_c = DCBQuery {
        items: vec![
            DCBQueryItem {
                types: vec![],
                tags: vec!["tagB".to_string()],
            },
            DCBQueryItem {
                types: vec![],
                tags: vec!["tagC".to_string()],
            },
        ],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_tag_b_or_c.clone()), None, false, None)
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event3.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Backwards
    let (result, head) = event_store
        .read_with_head(Some(query_tag_b_or_c.clone()), None, true, None)
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(event2.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Read events with tagX or tagY, expect one event.
    let query_tag_x_or_y = DCBQuery {
        items: vec![
            DCBQueryItem {
                types: vec![],
                tags: vec!["tagX".to_string()],
            },
            DCBQueryItem {
                types: vec![],
                tags: vec!["tagY".to_string()],
            },
        ],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_tag_x_or_y.clone()), None, false, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Backwards
    let (result, head) = event_store
        .read_with_head(Some(query_tag_x_or_y.clone()), None, true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event1.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read events with type2 and tagA, expect one event.
    let query_type2_tag_a = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["type2".to_string()],
            tags: vec!["tagA".to_string()],
        }],
    };
    let (result, head) = event_store
        .read_with_head(Some(query_type2_tag_a.clone()), None, false, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Backwards
    let (result, head) = event_store
        .read_with_head(Some(query_type2_tag_a.clone()), None, true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read events with type2 and tagA start 3, expect no events.
    let (result, head) = event_store
        .read_with_head(Some(query_type2_tag_a.clone()), Some(3), false, None)
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(3), head);

    // Backwards
    let (result, head) = event_store
        .read_with_head(Some(query_type2_tag_a.clone()), Some(3), true, None)
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Read events with type2 and tagB, or with type3 and tagC, expect two events.
    let query_type2_tag_b_or_type3_tagc = DCBQuery {
        items: vec![
            DCBQueryItem {
                types: vec!["type2".to_string()],
                tags: vec!["tagB".to_string()],
            },
            DCBQueryItem {
                types: vec!["type3".to_string()],
                tags: vec!["tagC".to_string()],
            },
        ],
    };
    let (result, head) = event_store
        .read_with_head(
            Some(query_type2_tag_b_or_type3_tagc.clone()),
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event3.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Backwards
    let (result, head) = event_store
        .read_with_head(
            Some(query_type2_tag_b_or_type3_tagc.clone()),
            None,
            true,
            None,
        )
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(event2.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Backwards limit 1
    let (result, head) = event_store
        .read_with_head(
            Some(query_type2_tag_b_or_type3_tagc.clone()),
            None,
            true,
            Some(1),
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Repeat with query items in different order, expect events in ascending order.
    let query_type3_tag_c_or_type2_tag_b = DCBQuery {
        items: vec![
            DCBQueryItem {
                types: vec!["type3".to_string()],
                tags: vec!["tagC".to_string()],
            },
            DCBQueryItem {
                types: vec!["type2".to_string()],
                tags: vec!["tagB".to_string()],
            },
        ],
    };
    let (result, head) = event_store
        .read_with_head(
            Some(query_type3_tag_c_or_type2_tag_b.clone()),
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event2.data, result[0].event.data);
    assert_eq!(event3.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Backwards
    let (result, head) = event_store
        .read_with_head(
            Some(query_type3_tag_c_or_type2_tag_b.clone()),
            None,
            true,
            None,
        )
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(event2.data, result[1].event.data);
    assert_eq!(Some(3), head);

    // Backwards limit 1
    let (result, head) = event_store
        .read_with_head(
            Some(query_type3_tag_c_or_type2_tag_b.clone()),
            None,
            true,
            Some(1),
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(event3.data, result[0].event.data);
    assert_eq!(Some(3), head);

    // Append must fail if recorded events match condition.
    let event4 = DCBEvent {
        event_type: "type4".to_string(),
        data: b"data4".to_vec(),
        tags: vec![],
        uuid: None,
    };

    // Fail because condition matches all.
    let new = vec![event4.clone()];
    let result = event_store.append(new.clone(), Some(DCBAppendCondition::default()), None);
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches all after 1.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: DCBQuery::default(),
            after: Some(1),
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches type1.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type1.clone(),
            after: None,
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches type2 after 1.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type2.clone(),
            after: Some(1),
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches tagX.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_x.clone(),
            after: None,
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches tagA after 1.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_a.clone(),
            after: Some(1),
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches type1 and tagX.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type1_tag_x.clone(),
            after: None,
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches type2 and tagA after 1.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type2_tag_a.clone(),
            after: Some(1),
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches tagA and tagB.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_a_and_b.clone(),
            after: None,
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches tagB or tagC.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_b_or_c.clone(),
            after: None,
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches tagX or tagY.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_tag_x_or_y.clone(),
            after: None,
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Fail because condition matches with type2 and tagB, or with type3 and tagC.
    let result = event_store.append(
        new.clone(),
        Some(DCBAppendCondition {
            fail_if_events_match: query_type2_tag_b_or_type3_tagc.clone(),
            after: None,
        }),
        None,
    );
    assert!(matches!(result, Err(DCBError::IntegrityError(_))));

    // Can append after 3.
    let position = event_store.append(new.clone(), None, None).unwrap();
    assert_eq!(4, position);

    // Can append match type_n.
    let query_type_n = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["typeN".to_string()],
            tags: vec![],
        }],
    };
    let position = event_store
        .append(
            new.clone(),
            Some(DCBAppendCondition {
                fail_if_events_match: query_type_n,
                after: None,
            }),
            None,
        )
        .unwrap();
    assert_eq!(5, position);

    // Can append match tagY.
    let position = event_store
        .append(
            new.clone(),
            Some(DCBAppendCondition {
                fail_if_events_match: query_tag_y.clone(),
                after: None,
            }),
            None,
        )
        .unwrap();
    assert_eq!(6, position);

    // Can append match type1 after 1.
    let position = event_store
        .append(
            new.clone(),
            Some(DCBAppendCondition {
                fail_if_events_match: query_type1.clone(),
                after: Some(1),
            }),
            None,
        )
        .unwrap();
    assert_eq!(7, position);

    // Can append match tagX after 1.
    let position = event_store
        .append(
            new.clone(),
            Some(DCBAppendCondition {
                fail_if_events_match: query_tag_x.clone(),
                after: Some(1),
            }),
            None,
        )
        .unwrap();
    assert_eq!(8, position);

    // Can append match type1 and tagX after 1.
    let position = event_store
        .append(
            new.clone(),
            Some(DCBAppendCondition {
                fail_if_events_match: query_type1_tag_x.clone(),
                after: Some(1),
            }),
            None,
        )
        .unwrap();
    assert_eq!(9, position);

    // Can append match tagX, after 1.
    let position = event_store
        .append(
            new.clone(),
            Some(DCBAppendCondition {
                fail_if_events_match: query_tag_x.clone(),
                after: Some(1),
            }),
            None,
        )
        .unwrap();
    assert_eq!(10, position);

    // Check it works with course subscription consistency boundaries and events.
    let student_id = format!("student1-{}", Uuid::new_v4());
    let student_registered = DCBEvent {
        event_type: "StudentRegistered".to_string(),
        data: format!(r#"{{"name": "Student1", "max_courses": 10}}"#).into_bytes(),
        tags: vec![student_id.clone()],
        uuid: None,
    };

    let course_id = format!("course1-{}", Uuid::new_v4());
    let course_registered = DCBEvent {
        event_type: "CourseRegistered".to_string(),
        data: format!(r#"{{"name": "Course1", "places": 10}}"#).into_bytes(),
        tags: vec![course_id.clone()],
        uuid: None,
    };

    let student_joined_course = DCBEvent {
        event_type: "StudentJoinedCourse".to_string(),
        data: format!(
            r#"{{"student_id": "{}", "course_id": "{}"}}"#,
            student_id, course_id
        )
        .into_bytes(),
        tags: vec![course_id.clone(), student_id.clone()],
        uuid: None,
    };

    let _position = event_store
        .append(
            vec![student_registered.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: DCBQuery {
                    items: vec![DCBQueryItem {
                        types: vec!["StudentRegistered".to_string()],
                        tags: student_registered.tags.clone(),
                    }],
                },
                after: Some(3),
            }),
            None,
        )
        .unwrap();

    let _position = event_store
        .append(
            vec![course_registered.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: DCBQuery {
                    items: vec![DCBQueryItem {
                        types: vec![],
                        tags: course_registered.tags.clone(),
                    }],
                },
                after: Some(3),
            }),
            None,
        )
        .unwrap();

    let _position = event_store
        .append(
            vec![student_joined_course.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: DCBQuery {
                    items: vec![DCBQueryItem {
                        types: vec![],
                        tags: student_joined_course.tags.clone(),
                    }],
                },
                after: Some(3),
            }),
            None,
        )
        .unwrap();

    // Read all
    let (result, head) = event_store.read_with_head(None, None, false, None).unwrap();
    assert_eq!(13, result.len());
    assert_eq!(result[10].event.event_type, student_registered.event_type);
    assert_eq!(result[11].event.event_type, course_registered.event_type);
    assert_eq!(
        result[12].event.event_type,
        student_joined_course.event_type
    );
    assert_eq!(result[10].event.data, student_registered.data);
    assert_eq!(result[11].event.data, course_registered.data);
    assert_eq!(result[12].event.data, student_joined_course.data);
    assert_eq!(result[10].event.tags, student_registered.tags);
    assert_eq!(result[11].event.tags, course_registered.tags);
    assert_eq!(result[12].event.tags, student_joined_course.tags);
    assert_eq!(Some(13), head);

    // Read all backwards
    let (result, head) = event_store.read_with_head(None, None, true, None).unwrap();
    assert_eq!(13, result.len());
    assert_eq!(result[2].event.event_type, student_registered.event_type);
    assert_eq!(result[1].event.event_type, course_registered.event_type);
    assert_eq!(result[0].event.event_type, student_joined_course.event_type);
    assert_eq!(result[2].event.data, student_registered.data);
    assert_eq!(result[1].event.data, course_registered.data);
    assert_eq!(result[0].event.data, student_joined_course.data);
    assert_eq!(result[2].event.tags, student_registered.tags);
    assert_eq!(result[1].event.tags, course_registered.tags);
    assert_eq!(result[0].event.tags, student_joined_course.tags);
    assert_eq!(Some(13), head);

    // Read student
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![student_id.clone()],
                }],
            }),
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(result[0].event.data, student_registered.data);
    assert_eq!(result[1].event.data, student_joined_course.data);
    assert_eq!(Some(13), head);

    // Read student backwards
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![student_id.clone()],
                }],
            }),
            None,
            true,
            None,
        )
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(result[1].event.data, student_registered.data);
    assert_eq!(result[0].event.data, student_joined_course.data);
    assert_eq!(Some(13), head);

    // Read course
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![course_id.clone()],
                }],
            }),
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(result[0].event.data, course_registered.data);
    assert_eq!(result[1].event.data, student_joined_course.data);
    assert_eq!(Some(13), head);

    // Read course backwards
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![course_id.clone()],
                }],
            }),
            None,
            true,
            None,
        )
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(result[1].event.data, course_registered.data);
    assert_eq!(result[0].event.data, student_joined_course.data);
    assert_eq!(Some(13), head);

    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![
                        student_joined_course.tags[0].clone(),
                        student_joined_course.tags[1].clone(),
                    ],
                }],
            }),
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(13), head);

    // Read student start position 3
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![student_id.clone()],
                }],
            }),
            Some(3),
            false,
            None,
        )
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(Some(13), head);

    // Read student backwards start position 3
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![student_id.clone()],
                }],
            }),
            Some(3),
            true,
            None,
        )
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(13), head);

    // Read course start position 3
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![course_id.clone()],
                }],
            }),
            Some(3),
            false,
            None,
        )
        .unwrap();
    assert_eq!(2, result.len());
    assert_eq!(Some(13), head);

    // Read course backwards start position 3
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![course_id.clone()],
                }],
            }),
            Some(3),
            true,
            None,
        )
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(13), head);

    // Read joined course start position 3
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![
                        student_joined_course.tags[0].clone(),
                        student_joined_course.tags[1].clone(),
                    ],
                }],
            }),
            Some(3),
            false,
            None,
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(13), head);

    // Read joined course backwards start position 3
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![
                        student_joined_course.tags[0].clone(),
                        student_joined_course.tags[1].clone(),
                    ],
                }],
            }),
            Some(3),
            true,
            None,
        )
        .unwrap();
    assert_eq!(0, result.len());
    assert_eq!(Some(13), head);

    // Read student start position 3 limit 1
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![student_id.clone()],
                }],
            }),
            Some(3),
            false,
            Some(1),
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(11), head);

    // Read student backwards start position 13 limit 1
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![student_id.clone()],
                }],
            }),
            Some(13),
            true,
            Some(1),
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(13), head);

    // Read course start position 3 limit 1
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![course_id.clone()],
                }],
            }),
            Some(3),
            false,
            Some(1),
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(12), head);

    // Read course backwards start position 13 limit 1
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![course_id.clone()],
                }],
            }),
            Some(13),
            true,
            Some(1),
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(13), head);

    // Read joined course start position 3 limit 1
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![
                        student_joined_course.tags[0].clone(),
                        student_joined_course.tags[1].clone(),
                    ],
                }],
            }),
            Some(3),
            false,
            Some(1),
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(13), head);

    // Read joined course backwards start position 13 limit 1
    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![
                        student_joined_course.tags[0].clone(),
                        student_joined_course.tags[1].clone(),
                    ],
                }],
            }),
            Some(13),
            true,
            Some(1),
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(13), head);

    // Read both students
    let consistency_boundary = DCBQuery {
        items: vec![
            DCBQueryItem {
                types: vec![
                    "StudentRegistered".to_string(),
                    "StudentJoinedCourse".to_string(),
                ],
                tags: vec![student_id.clone()],
            },
            DCBQueryItem {
                types: vec![
                    "CourseRegistered".to_string(),
                    "StudentJoinedCourse".to_string(),
                ],
                tags: vec![course_id.clone()],
            },
        ],
    };
    let (result, head) = event_store
        .read_with_head(Some(consistency_boundary.clone()), None, false, None)
        .unwrap();
    assert_eq!(3, result.len());
    assert_eq!(result[0].event.data, student_registered.data);
    assert_eq!(result[1].event.data, course_registered.data);
    assert_eq!(result[2].event.data, student_joined_course.data);
    assert_eq!(Some(13), head);

    // Read both students backwards
    let (result, head) = event_store
        .read_with_head(Some(consistency_boundary.clone()), None, true, None)
        .unwrap();
    assert_eq!(3, result.len());
    assert_eq!(result[2].event.data, student_registered.data);
    assert_eq!(result[1].event.data, course_registered.data);
    assert_eq!(result[0].event.data, student_joined_course.data);
    assert_eq!(Some(13), head);

    // Final test of head() method after all operations
    let head_position = event_store.head().unwrap();
    assert_eq!(Some(13), head_position);

    // Test UUID attribute is maintained.
    let event5 = DCBEvent {
        event_type: "type5".to_string(),
        data: b"data5".to_vec(),
        tags: vec!["tag5".to_string()],
        uuid: Some(Uuid::new_v4()),
    };

    let commit_position5 = event_store
        .append(
            vec![event5.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: DCBQuery {
                    items: vec![DCBQueryItem {
                        types: vec![],
                        tags: event5.tags.clone(),
                    }],
                },
                after: Some(13),
            }),
            None,
        )
        .unwrap();

    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: event5.tags.clone(),
                }],
            }),
            Some(14),
            false,
            None,
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(14), head);
    assert_eq!(event5.data, result[0].event.data);
    assert_eq!(event5.uuid, result[0].event.uuid);

    // Check UUID activates idempotency.
    let commit_position6 = event_store
        .append(
            vec![event5.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: DCBQuery {
                    items: vec![DCBQueryItem {
                        types: vec![],
                        tags: event5.tags.clone(),
                    }],
                },
                after: Some(13),
            }),
            None,
        )
        .unwrap();

    assert_eq!(commit_position5, commit_position6);

    let (result, head) = event_store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: event5.tags.clone(),
                }],
            }),
            Some(14),
            false,
            None,
        )
        .unwrap();
    assert_eq!(1, result.len());
    assert_eq!(Some(14), head);
    assert_eq!(event5.data, result[0].event.data);
    assert_eq!(event5.uuid, result[0].event.uuid);
}

#[test]
fn test_direct_event_store() {
    let temp_dir = tempdir().unwrap();
    let event_store = UmaDB::new(temp_dir.path()).unwrap();
    dcb_event_store_test(&event_store);
}

// Also test gRPC by wrapping the async client with a small sync adapter used only in tests.
#[test]
fn test_grpc_event_store_client() {
    // Pick a free port
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let addr_noscheme = format!("{}", addr);
    let addr_with_scheme = format!("http://{}", addr);

    // Build a multi-threaded runtime for server and client
    let rt = RtBuilder::new_multi_thread().enable_all().build().unwrap();

    // Prepare shutdown channel for the server
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    // Temporary directory for the database server
    let temp_dir = tempdir().unwrap();

    // Start the server
    let data_dir = temp_dir.path().to_path_buf();
    rt.spawn(async move {
        let _ = start_server(data_dir, &addr_noscheme, shutdown_rx).await;
    });

    // Connect the client (retry until server is ready)
    let client = {
        use std::{thread, time::Duration};
        let mut attempts = 0;
        loop {
            match UmaDBClient::new(addr_with_scheme.clone()).connect() {
                Ok(c) => break c,
                Err(e) => {
                    attempts += 1;
                    if attempts >= 50 {
                        panic!("failed to connect to grpc server after retries: {:?}", e);
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    };

    dcb_event_store_test(&client);

    // Shutdown server
    let _ = shutdown_tx.send(());
}

#[test]
fn test_tag_hash_collision() {
    // Known colliding tags (by the system's tag hashing scheme)
    let student_tag = "student-1a5c7f43-c0cb-465f-a9ad-8e452cce2b38".to_string();
    let course_tag = "course-2b99b9c4-9031-43fd-a7e5-dceda7cab3ea".to_string();

    // Use a local EventStore backed by a temporary directory
    let temp_dir = tempdir().unwrap();
    let store = UmaDB::new(temp_dir.path()).unwrap();

    // Append two events, one per colliding tag
    let ev_student = DCBEvent {
        event_type: "StudentEvent".to_string(),
        data: b"student-data".to_vec(),
        tags: vec![student_tag.clone()],
        uuid: None,
    };
    let ev_course = DCBEvent {
        event_type: "CourseEvent".to_string(),
        data: b"course-data".to_vec(),
        tags: vec![course_tag.clone()],
        uuid: None,
    };

    let _ = store.append(vec![ev_student.clone()], None, None).unwrap();
    let _ = store.append(vec![ev_course.clone()], None, None).unwrap();

    // Query by student tag: should return only the student event
    let (result, _head) = store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![student_tag.clone()],
                }],
            }),
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(
        1,
        result.len(),
        "Query by student tag should return exactly 1 event"
    );
    assert_eq!(ev_student.event_type, result[0].event.event_type);
    assert_eq!(ev_student.tags, result[0].event.tags);

    // Query by course tag: should return only the course event
    let (result, _head) = store
        .read_with_head(
            Some(DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec![course_tag.clone()],
                }],
            }),
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(
        1,
        result.len(),
        "Query by course tag should return exactly 1 event"
    );
    assert_eq!(ev_course.event_type, result[0].event.event_type);
    assert_eq!(ev_course.tags, result[0].event.tags);

    // Query with two items (student tag OR course tag): should return both events
    let (result, _head) = store
        .read_with_head(
            Some(DCBQuery {
                items: vec![
                    DCBQueryItem {
                        types: vec![],
                        tags: vec![student_tag.clone()],
                    },
                    DCBQueryItem {
                        types: vec![],
                        tags: vec![course_tag.clone()],
                    },
                ],
            }),
            None,
            false,
            None,
        )
        .unwrap();
    assert_eq!(
        2,
        result.len(),
        "OR query across both tags should return 2 events"
    );
    let types: Vec<String> = result.into_iter().map(|e| e.event.event_type).collect();
    assert!(types.contains(&"StudentEvent".to_string()));
    assert!(types.contains(&"CourseEvent".to_string()));
}
