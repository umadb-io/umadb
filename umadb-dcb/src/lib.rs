//! API for Dynamic Consistency Boundaries (DCB) event store
//!
//! This module provides the core interfaces and data structures for working with
//! an event store that supports dynamic consistency boundaries.

use async_trait::async_trait;
use futures_core::Stream;
use futures_util::StreamExt;
use std::iter::Iterator;
use thiserror::Error;
use uuid::Uuid;

/// Non-async Rust interface for recording and retrieving events
pub trait DCBEventStoreSync {
    /// Reads events from the store based on the provided query and constraints
    ///
    /// Returns a DCBReadResponseSync that provides an iterator over all events,
    /// unless 'from' is given then only those with position greater than 'after',
    /// and unless any query items are given, then only those that match at least one
    /// query item. An event matches a query item if its type is in the item types or
    /// there are no item types, and if all the item tags are in the event tags.
    fn read(
        &self,
        query: Option<DCBQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool,
    ) -> DCBResult<Box<dyn DCBReadResponseSync + 'static>>;

    /// Reads events from the store and returns them as a tuple of (Vec<DCBSequencedEvent>, Option<u64>)
    fn read_with_head(
        &self,
        query: Option<DCBQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let mut response = self.read(query, start, backwards, limit, false)?;
        response.collect_with_head()
    }

    /// Returns the current head position of the event store, or None if empty
    ///
    /// Returns the value of last_committed_position, or None if last_committed_position is zero
    fn head(&self) -> DCBResult<Option<u64>>;

    /// Appends given events to the event store, unless the condition fails
    ///
    /// Returns the position of the last appended event
    fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64>;
}

/// Response from a read operation, providing an iterator over sequenced events
pub trait DCBReadResponseSync: Iterator<Item = Result<DCBSequencedEvent, DCBError>> {
    /// Returns the current head position of the event store, or None if empty
    fn head(&mut self) -> DCBResult<Option<u64>>;
    /// Returns a vector of events with head
    fn collect_with_head(&mut self) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)>;
    /// Returns a batch of events, updating head with the last event in the batch if there is one and if limit.is_some() is true
    fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>>;
}

/// Async Rust interface for recording and retrieving events
#[async_trait]
pub trait DCBEventStoreAsync: Send + Sync {
    /// Reads events from the store based on the provided query and constraints
    ///
    /// Returns a DCBReadResponseSync that provides an iterator over all events,
    /// unless 'after' is given then only those with position greater than 'after',
    /// and unless any query items are given, then only those that match at least one
    /// query item. An event matches a query item if its type is in the item types or
    /// there are no item types, and if all the item tags are in the event tags.
    async fn read<'a>(
        &'a self,
        query: Option<DCBQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool,
    ) -> DCBResult<Box<dyn DCBReadResponseAsync + Send + 'static>>;

    /// Reads events from the store and returns them as a tuple of (Vec<DCBSequencedEvent>, Option<u64>)
    async fn read_with_head<'a>(
        &'a self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let mut response = self.read(query, after, backwards, limit, false).await?;
        response.collect_with_head().await
    }

    /// Returns the current head position of the event store, or None if empty
    ///
    /// Returns the value of last_committed_position, or None if last_committed_position is zero
    async fn head(&self) -> DCBResult<Option<u64>>;

    /// Appends given events to the event store, unless the condition fails
    ///
    /// Returns the position of the last appended event
    async fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64>;
}

/// Asynchronous response from a read operation, providing a stream of sequenced events
#[async_trait]
pub trait DCBReadResponseAsync: Stream<Item = DCBResult<DCBSequencedEvent>> + Send + Unpin {
    async fn head(&mut self) -> DCBResult<Option<u64>>;

    async fn collect_with_head(&mut self) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let mut events = Vec::new();
        while let Some(result) = self.next().await {
            events.push(result?); // propagate error from stream
        }

        let head = self.head().await?;
        Ok((events, head))
    }

    async fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>>;
}

/// Represents a query item for filtering events
#[derive(Debug, Clone, Default)]
pub struct DCBQueryItem {
    /// Event types to match
    pub types: Vec<String>,
    /// Tags that must all be present in the event
    pub tags: Vec<String>,
}

impl DCBQueryItem {
    /// Creates a new query item
    pub fn new() -> Self {
        Self {
            types: vec![],
            tags: vec![],
        }
    }

    /// Sets the types for this query item
    pub fn types<I, S>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.types = types.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Sets the tags for this query item
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = tags.into_iter().map(|s| s.into()).collect();
        self
    }
}

/// A query composed of multiple query items
#[derive(Debug, Clone, Default)]
pub struct DCBQuery {
    /// List of query items, where events matching any item are included in results
    pub items: Vec<DCBQueryItem>,
}

impl DCBQuery {
    /// Creates a new empty query
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// Creates a query with the specified items
    pub fn with_items<I>(items: I) -> Self
    where
        I: IntoIterator<Item = DCBQueryItem>,
    {
        Self {
            items: items.into_iter().collect(),
        }
    }

    /// Adds a query item to this query
    pub fn item(mut self, item: DCBQueryItem) -> Self {
        self.items.push(item);
        self
    }

    /// Adds multiple query items to this query
    pub fn items<I>(mut self, items: I) -> Self
    where
        I: IntoIterator<Item = DCBQueryItem>,
    {
        self.items.extend(items);
        self
    }
}

/// Conditions that must be satisfied for an append operation to succeed
#[derive(Debug, Clone, Default)]
pub struct DCBAppendCondition {
    /// Query that, if matching any events, will cause the append to fail
    pub fail_if_events_match: DCBQuery,
    /// Position after which to append; if None, append at the end
    pub after: Option<u64>,
}

impl DCBAppendCondition {
    /// Creates a new empty append condition
    pub fn new(fail_if_events_match: DCBQuery) -> Self {
        Self {
            fail_if_events_match,
            after: None,
        }
    }

    pub fn after(mut self, after: Option<u64>) -> Self {
        self.after = after;
        self
    }
}

/// Represents an event in the event store
#[derive(Debug, Clone)]
pub struct DCBEvent {
    /// Type of the event
    pub event_type: String,
    /// Binary data associated with the event
    pub data: Vec<u8>,
    /// Tags associated with the event
    pub tags: Vec<String>,
    /// Unique event ID
    pub uuid: Option<Uuid>,
}

impl Default for DCBEvent {
    fn default() -> Self {
        Self::new()
    }
}

impl DCBEvent {
    /// Creates a new event
    pub fn new() -> Self {
        Self {
            event_type: "".to_string(),
            data: Vec::new(),
            tags: Vec::new(),
            uuid: None,
        }
    }

    /// Sets the type for this event
    pub fn event_type<S: Into<String>>(mut self, event_type: S) -> Self {
        self.event_type = event_type.into();
        self
    }

    /// Sets the data for this event
    pub fn data<D: Into<Vec<u8>>>(mut self, data: D) -> Self {
        self.data = data.into();
        self
    }

    /// Sets the tags for this event
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = tags.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Sets the UUID for this event
    pub fn uuid(mut self, uuid: Uuid) -> Self {
        self.uuid = Some(uuid);
        self
    }
}

/// An event with its position in the event sequence
#[derive(Debug, Clone)]
pub struct DCBSequencedEvent {
    /// The event
    pub event: DCBEvent,
    /// Position of the event in the sequence
    pub position: u64,
}

// Error types
#[derive(Error, Debug)]
pub enum DCBError {
    // Generic/system errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    // DCB domain errors
    #[error("Integrity error: condition failed: {0}")]
    IntegrityError(String),
    #[error("Corruption detected: {0}")]
    Corruption(String),

    // LMDB/Storage domain errors (unified into DCBError)
    #[error("Page not found: {0:?}")]
    PageNotFound(u64),
    #[error("Dirty page not found: {0:?}")]
    DirtyPageNotFound(u64),
    #[error("Root ID mismatched: old {0:?} new {1:?}")]
    RootIDMismatch(u64, u64),
    #[error("Database corrupted: {0}")]
    DatabaseCorrupted(String),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    #[error("Page already freed: {0:?}")]
    PageAlreadyFreed(u64),
    #[error("Page already dirty: {0:?}")]
    PageAlreadyDirty(u64),
    #[error("Transport error: {0}")]
    TransportError(String),
    #[error("Cancelled by user")]
    CancelledByUser(),
}

pub type DCBResult<T> = Result<T, DCBError>;

#[cfg(test)]
mod tests {
    use super::*;

    // A simple implementation of DCBReadResponseSync for testing
    struct TestReadResponse {
        events: Vec<DCBSequencedEvent>,
        current_index: usize,
        head_position: Option<u64>,
    }

    impl TestReadResponse {
        fn new(events: Vec<DCBSequencedEvent>, head_position: Option<u64>) -> Self {
            Self {
                events,
                current_index: 0,
                head_position,
            }
        }
    }

    impl Iterator for TestReadResponse {
        type Item = Result<DCBSequencedEvent, DCBError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.current_index < self.events.len() {
                let event = self.events[self.current_index].clone();
                self.current_index += 1;
                Some(Ok(event))
            } else {
                None
            }
        }
    }

    impl DCBReadResponseSync for TestReadResponse {
        fn head(&mut self) -> DCBResult<Option<u64>> {
            Ok(self.head_position)
        }

        fn collect_with_head(&mut self) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
            todo!()
        }

        fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
            let mut batch = Vec::new();
            while let Some(result) = self.next() {
                match result {
                    Ok(event) => batch.push(event),
                    Err(err) => {
                        panic!("{}", err);
                    }
                }
            }
            Ok(batch)
        }
    }

    #[test]
    fn test_dcb_read_response() {
        // Create some test events
        let event1 = DCBEvent {
            event_type: "test_event".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
            uuid: None,
        };

        let event2 = DCBEvent {
            event_type: "another_event".to_string(),
            data: vec![4, 5, 6],
            tags: vec!["tag2".to_string(), "tag3".to_string()],
            uuid: None,
        };

        let seq_event1 = DCBSequencedEvent {
            event: event1,
            position: 1,
        };

        let seq_event2 = DCBSequencedEvent {
            event: event2,
            position: 2,
        };

        // Create a test response
        let mut response =
            TestReadResponse::new(vec![seq_event1.clone(), seq_event2.clone()], Some(2));

        // Test head position
        assert_eq!(response.head().unwrap(), Some(2));

        // Test iterator functionality
        assert_eq!(response.next().unwrap().unwrap().position, 1);
        assert_eq!(response.next().unwrap().unwrap().position, 2);
        assert!(response.next().is_none());
    }

    #[test]
    fn test_event_new() {
        let event1 = DCBEvent::default()
            .event_type("type1")
            .data(b"data1")
            .tags(["tagX"]);

        // println!("Event created with builder API:");
        // println!("  event_type: {}", event1.event_type);
        // println!("  data: {:?}", event1.data);
        // println!("  tags: {:?}", event1.tags);
        // println!("  uuid: {:?}", event1.uuid);

        // Verify the fields match expectations
        assert_eq!(event1.event_type, "type1");
        assert_eq!(event1.data, b"data1".to_vec());
        assert_eq!(event1.tags, vec!["tagX".to_string()]);
        assert_eq!(event1.uuid, None);

        // Test with multiple tags
        let event2 = DCBEvent::default()
            .event_type("type2")
            .data(b"data2")
            .tags(["tag1", "tag2", "tag3"]);
        assert_eq!(event2.tags.len(), 3);

        // Test without data or tags
        let event3 = DCBEvent::default().event_type("type3");
        assert_eq!(event3.data.len(), 0);
        assert_eq!(event3.tags.len(), 0);

        // Test DCBQueryItem builder
        let query_item = DCBQueryItem::new()
            .types(["type1", "type2"])
            .tags(["tagA", "tagB"]);
        assert_eq!(query_item.types.len(), 2);
        assert_eq!(query_item.tags.len(), 2);

        // Test DCBQuery builder
        let query = DCBQuery::new().item(query_item);
        assert_eq!(query.items.len(), 1);

        println!("\nAll builder API tests passed!");
    }
}
