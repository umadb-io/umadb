use umadb_client::UmaDBClient;
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreSync, DCBQuery, DCBQueryItem, TrackingInfo,
};
use uuid::Uuid;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the gRPC server
    let url = "http://localhost:50051".to_string();
    let client = UmaDBClient::new(url).connect()?;

    // Define a consistency boundary
    let boundary = DCBQuery::new().item(
        DCBQueryItem::new()
            .types(["example"])
            .tags(["tag1", "tag2"]),
    );

    // Read events for a decision model
    let mut read_response = client.read(Some(boundary.clone()), None, false, None, false)?;

    // Build decision model
    while let Some(result) = read_response.next() {
        match result {
            Ok(event) => {
                println!(
                    "Got event at position {}: {:?}",
                    event.position, event.event
                );
            }
            Err(status) => panic!("gRPC stream error: {}", status),
        }
    }

    // Remember the last-known position
    let last_known_position = read_response.head().unwrap();
    println!("Last known position is: {:?}", last_known_position);

    // Produce new event
    let event = DCBEvent::default()
        .event_type("example")
        .tags(["tag1", "tag2"])
        .data(b"Hello, world!")
        .uuid(Uuid::new_v4());

    // Append event in consistency boundary
    let append_condition = DCBAppendCondition {
        fail_if_events_match: boundary.clone(),
        after: last_known_position,
    };
    let position1 = client.append(vec![event.clone()], Some(append_condition.clone()), None)?;

    println!("Appended event at position: {}", position1);

    // Append conflicting event - expect an error
    let conflicting_event = DCBEvent::default()
        .event_type("example")
        .tags(["tag1", "tag2"])
        .data(b"Hello, world!")
        .uuid(Uuid::new_v4()); // different UUID

    let conflicting_result = client.append(
        vec![conflicting_event],
        Some(append_condition.clone()),
        None,
    );

    // Expect an integrity error
    match conflicting_result {
        Err(DCBError::IntegrityError(integrity_error)) => {
            println!("Conflicting event was rejected: {:?}", integrity_error);
        }
        other => panic!("Expected IntegrityError, got {:?}", other),
    }

    // Appending with identical event IDs and append condition is idempotent.
    println!(
        "Retrying to append event at position: {:?}",
        last_known_position
    );
    let position2 = client.append(vec![event.clone()], Some(append_condition.clone()), None)?;

    if position1 == position2 {
        println!("Append method returned same commit position: {}", position2);
    } else {
        panic!("Expected idempotent retry!")
    }

    // Subscribe to all events for a projection
    let mut subscription = client.read(None, None, false, None, true)?;

    // Build an up-to-date view
    while let Some(result) = subscription.next() {
        match result {
            Ok(ev) => {
                println!("Processing event at {}: {:?}", ev.position, ev.event);
                if ev.position == position2 {
                    println!("Projection has processed new event!");
                    break;
                }
            }
            Err(status) => panic!("gRPC stream error: {}", status),
        }
    }

    // Track an upstream position
    let upstream_position = client.get_tracking_info("upstream")?;
    let next_upstream_position = upstream_position.unwrap_or(0) + 1;
    println!("Next upstream position: {next_upstream_position}");
    client.append(
        vec![],
        None,
        Some(TrackingInfo {
            source: "upstream".to_string(),
            position: next_upstream_position,
        }),
    )?;
    assert_eq!(
        next_upstream_position,
        client.get_tracking_info("upstream")?.unwrap()
    );
    println!("Upstream position tracked okay!");

    // Try recording the same upstream position
    let conflicting_result = client.append(
        vec![],
        None,
        Some(TrackingInfo {
            source: "upstream".to_string(),
            position: next_upstream_position,
        }),
    );

    // Expect an integrity error
    match conflicting_result {
        Err(DCBError::IntegrityError(integrity_error)) => {
            println!(
                "Conflicting upstream position was rejected: {:?}",
                integrity_error
            );
        }
        other => panic!("Expected IntegrityError, got {:?}", other),
    }

    Ok(())
}
