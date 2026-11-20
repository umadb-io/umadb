use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use serial_test::serial;
use tests_integration::TempServer;
use umadb_client::UmaDbClient;
use umadb_dcb::{DcbEvent, DcbEventStoreSync};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_subscribe_at_head() {
    let server = TempServer::start_insecure().await;
    let http_url = server.url.clone();

    tokio::task::spawn_blocking(move || {
        // Build client with small batch_size so we can test next_batch
        let client = UmaDbClient::new(http_url)
            .connect()
            .expect("connect sync client");

        // 1. Append an initial event
        let event = DcbEvent {
            event_type: "TestEvent".to_string(),
            data: b"hello".to_vec(),
            tags: vec!["tag1".to_string()],
            uuid: None,
            metadata: Vec::new(),
        };
        client
            .append(vec![event.clone()], None, None)
            .expect("append 1");

        // 2. Get the current head
        let head = client.head().expect("get head");
        assert!(head.is_some());
        let head_val = head.unwrap();
        println!("Current head: {}", head_val);

        // 3. Subscribe from head
        let mut subscription = client.subscribe(None, Some(head_val)).expect("subscribe");

        // 4. Try to get next event in a separate thread because it might block (or should block)
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();

        let next_event_handle = std::thread::spawn(move || {
            let res = subscription.next();
            finished_clone.store(true, Ordering::SeqCst);
            res
        });

        // Wait a bit to see if it finishes immediately
        std::thread::sleep(std::time::Duration::from_millis(500));

        if finished.load(Ordering::SeqCst) {
            let res = next_event_handle.join().unwrap();
            if res.is_none() {
                panic!("Subscription ended immediately with None (StopIteration)!");
            } else {
                println!("Subscription returned something: {:?}", res);
            }
        } else {
            println!("Subscription is blocking as expected.");

            // 5. Append a new event to trigger the subscription
            client
                .append(vec![event.clone()], None, None)
                .expect("append 2");

            // 6. Now it should finish
            let res = next_event_handle.join().unwrap();
            assert!(res.is_some());
            println!("Received event: {:?}", res.unwrap().unwrap().position);
        }
    }).await.unwrap();

    // Cleanup
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_read_subscribe_after_head() {
    let server = TempServer::start_insecure().await;
    let http_url = server.url.clone();

    tokio::task::spawn_blocking(move || {
        // Build client with small batch_size so we can test next_batch
        let client = UmaDbClient::new(http_url)
            .connect()
            .expect("connect sync client");

        // 1. Append an initial event
        let event = DcbEvent {
            event_type: "TestEvent".to_string(),
            data: b"hello".to_vec(),
            tags: vec!["tag1".to_string()],
            uuid: None,
            metadata: Vec::new(),
        };
        client
            .append(vec![event.clone()], None, None)
            .expect("append 1");

        // 2. Get the current head
        let head = client.head().expect("get head").expect("head exists");
        println!("Current head: {}", head);

        // 3. Subscribe after head.
        let mut subscription = client.subscribe(None, Some(head)).expect("read subscribe");

        // 4. Try to get next event in a separate thread
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();

        let next_event_handle = std::thread::spawn(move || {
            let res = subscription.next();
            finished_clone.store(true, Ordering::SeqCst);
            res
        });

        // Wait a bit to see if it finishes immediately
        std::thread::sleep(std::time::Duration::from_millis(500));

        if finished.load(Ordering::SeqCst) {
            let res = next_event_handle.join().unwrap();
            if res.is_none() {
                panic!("Subscription (read subscribe) ended immediately with None (StopIteration)!");
            } else {
                println!("Subscription returned something: {:?}", res);
            }
        } else {
            println!("Subscription is blocking as expected.");

            // 5. Append a new event to trigger the subscription
            client
                .append(vec![event.clone()], None, None)
                .expect("append 2");

            // 6. Now it should finish
            let res = next_event_handle.join().unwrap();
            assert!(res.is_some());
            println!("Received event: {:?}", res.unwrap().unwrap().position);
        }
    }).await.unwrap();

    // Cleanup
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_read_limit_head_consistency() {
    let server = TempServer::start_insecure().await;
    let http_url = server.url.clone();

    tokio::task::spawn_blocking(move || {
        // Build client with small batch_size so we can test next_batch
        let client = UmaDbClient::new(http_url.clone())
            .connect()
            .expect("connect sync client");

        // 1. Append 3 events
        let event = DcbEvent {
            event_type: "TestEvent".to_string(),
            data: b"hello".to_vec(),
            tags: vec![],
            uuid: None,
            metadata: Vec::new(),
        };
        client
            .append(
                vec![event.clone(), event.clone(), event.clone()],
                None,
                None,
            )
            .expect("append 3");

        let head = client.head().expect("get head").expect("head exists");
        assert_eq!(head, 3);

        // 2. Read with limit=None, but batch_size=2
        let client2 = UmaDbClient::new(http_url.clone())
            .batch_size(2)
            .connect()
            .expect("client 2 connect");

        let mut response = client2
            .read(None, Some(1), false, None)
            .expect("read unlimited");

        let batch = response.next_batch().expect("next batch");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].position, 1);
        assert_eq!(batch[1].position, 2);

        // 3. Check reported head.
        // let reported_head = response.head().expect("get reported head");
        // TODO: Not sure if this (generated by Gemini Flash 3 is true):
        // assert_eq!(reported_head, Some(2), "Head should be the position of the last event in the batch, even for unlimited reads");
    }).await.unwrap();

    // Cleanup
    server.shutdown().await;
}
