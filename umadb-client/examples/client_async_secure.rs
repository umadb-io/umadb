use futures::StreamExt;
use umadb_client::UmaDBClient;
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreAsync, DCBQuery, DCBQueryItem,
};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the gRPC server
    let url = "https://localhost:50051".to_string();
    let client = UmaDBClient::new(url)
        .ca_path("server.pem".to_string()) // For self-signed server certificates.
        .api_key("umadb:example-api-key-4f7c2b1d9e5f4a038c1a".to_string())
        .connect_async()
        .await?;

    // Define a consistency boundary
    let cb = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["example".to_string()],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        }],
    };

    // Read events for a decision model
    let mut read_response = client
        .read(Some(cb.clone()), None, false, None, false)
        .await?;

    // Build decision model
    while let Some(result) = read_response.next().await {
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
    let last_known_position = read_response.head().await?;
    println!("Last known position is: {:?}", last_known_position);

    // Produce new event
    let event = DCBEvent {
        event_type: "example".to_string(),
        tags: vec!["tag1".to_string(), "tag2".to_string()],
        data: b"Hello, world!".to_vec(),
        uuid: Some(Uuid::new_v4()),
    };

    // Append event in consistency boundary
    let commit_position1 = client
        .append(
            vec![event.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: cb.clone(),
                after: last_known_position,
            }),
        )
        .await?;

    println!("Appended event at position: {}", commit_position1);

    // Append conflicting event - expect an error
    let conflicting_event = DCBEvent {
        event_type: "example".to_string(),
        tags: vec!["tag1".to_string(), "tag2".to_string()],
        data: b"Hello, world!".to_vec(),
        uuid: Some(Uuid::new_v4()), // different UUID
    };

    let conflicting_result = client
        .append(
            vec![conflicting_event.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: cb.clone(),
                after: last_known_position,
            }),
        )
        .await;

    // Expect an integrity error
    match conflicting_result {
        Err(DCBError::IntegrityError(integrity_error)) => {
            println!("Conflicting event was rejected: {:?}", integrity_error);
        }
        other => panic!("Expected IntegrityError, got {:?}", other),
    }

    // Appending with identical events IDs and append conditions is idempotent.
    println!(
        "Retrying to append event at position: {:?}",
        last_known_position
    );
    let commit_position2 = client
        .append(
            vec![event.clone()],
            Some(DCBAppendCondition {
                fail_if_events_match: cb.clone(),
                after: last_known_position,
            }),
        )
        .await?;

    if commit_position1 == commit_position2 {
        println!(
            "Append method returned same commit position: {}",
            commit_position2
        );
    } else {
        panic!("Expected idempotent retry!")
    }

    // Subscribe to all events for a projection
    let mut subscription = client.read(None, None, false, None, true).await?;

    // Build an up-to-date view
    while let Some(result) = subscription.next().await {
        match result {
            Ok(ev) => {
                println!("Processing event at {}: {:?}", ev.position, ev.event);
                if ev.position == commit_position2 {
                    println!("Projection has processed new event!");
                    break;
                }
            }
            Err(status) => panic!("gRPC stream error: {}", status),
        }
    }
    Ok(())
}
