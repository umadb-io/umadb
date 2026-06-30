use prost::Message;
use prost::bytes::Bytes;
use tonic::{Code, Status};
use umadb_dcb::{
    DcbAppendCondition, DcbError, DcbEvent, DcbQuery, DcbQueryItem, DcbResult, DcbSequencedEvent,
    TrackingInfo,
};
use uuid::Uuid;

// Include the generated proto code
pub mod v1 {
    tonic::include_proto!("umadb.v1");
}

// Conversion functions between proto and API types

impl From<v1::TrackingInfo> for TrackingInfo {
    fn from(t: v1::TrackingInfo) -> Self {
        TrackingInfo {
            source: t.source,
            position: t.position,
        }
    }
}

impl From<TrackingInfo> for v1::TrackingInfo {
    fn from(t: TrackingInfo) -> Self {
        v1::TrackingInfo {
            source: t.source,
            position: t.position,
        }
    }
}

impl TryFrom<v1::Event> for DcbEvent {
    type Error = DcbError;

    fn try_from(proto: v1::Event) -> DcbResult<Self> {
        // Parse UUID only if it's not empty, returning an error if it fails
        let uuid = Some(proto.uuid)
            .filter(|s| !s.is_empty())
            .map(|s| {
                Uuid::parse_str(&s).map_err(|_| {
                    DcbError::DeserializationError("Invalid UUID in Event".to_string())
                })
            })
            .transpose()?;

        // Convert Vec<MetadataEntry> to Vec<(String, String)>
        let metadata = proto
            .metadata
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect();

        Ok(DcbEvent {
            event_type: proto.event_type,
            tags: proto.tags,
            data: proto.data,
            uuid,
            metadata,
        })
    }
}

impl From<DcbEvent> for v1::Event {
    fn from(event: DcbEvent) -> Self {
        // Convert Vec<(String, String)> into Vec<v1::MetadataEntry>
        let metadata = event
            .metadata
            .into_iter()
            .map(|(key, value)| v1::MetadataEntry { key, value })
            .collect();

        v1::Event {
            event_type: event.event_type,
            tags: event.tags,
            data: event.data,
            uuid: event.uuid.map(|u| u.to_string()).unwrap_or_default(),
            metadata,
        }
    }
}

impl From<v1::QueryItem> for DcbQueryItem {
    fn from(proto: v1::QueryItem) -> Self {
        DcbQueryItem {
            types: proto.types,
            tags: proto.tags,
        }
    }
}

impl From<DcbQueryItem> for v1::QueryItem {
    fn from(item: DcbQueryItem) -> Self {
        v1::QueryItem {
            types: item.types,
            tags: item.tags,
        }
    }
}

impl From<v1::Query> for DcbQuery {
    fn from(proto: v1::Query) -> Self {
        DcbQuery {
            items: proto.items.into_iter().map(|item| item.into()).collect(),
        }
    }
}

impl From<DcbQuery> for v1::Query {
    fn from(query: DcbQuery) -> Self {
        v1::Query {
            items: query.items.into_iter().map(|item| item.into()).collect(),
        }
    }
}

impl From<v1::AppendCondition> for DcbAppendCondition {
    fn from(proto: v1::AppendCondition) -> Self {
        DcbAppendCondition {
            fail_if_events_match: proto
                .fail_if_events_match
                .map_or_else(DcbQuery::default, |q| q.into()),
            after: proto.after,
        }
    }
}

impl From<DcbSequencedEvent> for v1::SequencedEvent {
    fn from(event: DcbSequencedEvent) -> Self {
        v1::SequencedEvent {
            position: event.position,
            event: Some(event.event.into()),
        }
    }
}

// Helper: map DCBError -> tonic::Status with structured details
pub fn status_from_dcb_error(e: DcbError) -> Status {
    let (code, error_type) = match e {
        DcbError::AuthenticationError(_) => (
            Code::Unauthenticated,
            v1::error_response::ErrorType::Authentication as i32,
        ),
        DcbError::InvalidArgument(_) => (
            Code::InvalidArgument,
            v1::error_response::ErrorType::InvalidArgument as i32,
        ),
        DcbError::IntegrityError(_) => (
            Code::FailedPrecondition,
            v1::error_response::ErrorType::Integrity as i32,
        ),
        DcbError::Corruption(_)
        | DcbError::DatabaseCorrupted(_)
        | DcbError::DeserializationError(_) => (
            Code::DataLoss,
            v1::error_response::ErrorType::Corruption as i32,
        ),
        DcbError::SerializationError(_) => (
            Code::InvalidArgument,
            v1::error_response::ErrorType::Serialization as i32,
        ),
        DcbError::InternalError(_) => (
            Code::Internal,
            v1::error_response::ErrorType::Internal as i32,
        ),
        _ => (Code::Internal, v1::error_response::ErrorType::Io as i32),
    };
    let msg = e.to_string();
    let detail = v1::ErrorResponse {
        message: msg.clone(),
        error_type,
    };
    let bytes = detail.encode_to_vec();
    Status::with_details(code, msg, Bytes::from(bytes))
}

// Helper: map tonic::Status -> DCBError by decoding details
pub fn dcb_error_from_status(status: Status) -> DcbError {
    let details = status.details();
    // Try to decode ErrorResponse directly from details
    if !details.is_empty()
        && let Ok(err) = v1::ErrorResponse::decode(details)
    {
        return match err.error_type {
            x if x == v1::error_response::ErrorType::Authentication as i32 => {
                DcbError::AuthenticationError(err.message)
            }
            x if x == v1::error_response::ErrorType::InvalidArgument as i32 => {
                DcbError::InvalidArgument(err.message)
            }
            x if x == v1::error_response::ErrorType::Integrity as i32 => {
                DcbError::IntegrityError(err.message)
            }
            x if x == v1::error_response::ErrorType::Corruption as i32 => {
                DcbError::Corruption(err.message)
            }
            x if x == v1::error_response::ErrorType::Serialization as i32 => {
                DcbError::SerializationError(err.message)
            }
            x if x == v1::error_response::ErrorType::Internal as i32 => {
                DcbError::InternalError(err.message)
            }
            _ => DcbError::Io(std::io::Error::other(err.message)),
        };
    }
    // Fallback: infer from gRPC code
    match status.code() {
        Code::Unauthenticated => DcbError::AuthenticationError(status.message().to_string()),
        Code::InvalidArgument => DcbError::InvalidArgument(status.message().to_string()),
        Code::FailedPrecondition => DcbError::IntegrityError(status.message().to_string()),
        Code::DataLoss => DcbError::Corruption(status.message().to_string()),
        Code::Internal => DcbError::InternalError(status.message().to_string()),
        _ => DcbError::Io(std::io::Error::other(format!("gRPC error: {}", status))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_metadata_roundtrips_through_proto() {
        let mut metadata = Vec::new();
        metadata.push(("source".to_string(), "web".to_string()));
        metadata.push(("correlation_id".to_string(), "abc-123".to_string()));

        let event = DcbEvent {
            event_type: "Created".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["t1".to_string()],
            uuid: Some(Uuid::new_v4()),
            metadata: metadata.clone(),
        };

        // DcbEvent -> proto::Event carries the metadata map.
        let proto: v1::Event = event.clone().into();

        // proto::Event -> DcbEvent restores it.
        let round = DcbEvent::try_from(proto).unwrap();
        assert_eq!(event.event_type, round.event_type);
        assert_eq!(event.data, round.data);
        assert_eq!(event.tags, round.tags);
        assert_eq!(event.uuid, round.uuid);
        assert_eq!(metadata, round.metadata);
    }

    #[test]
    fn empty_metadata_roundtrips_through_proto() {
        let event = DcbEvent {
            event_type: "Created".to_string(),
            data: vec![],
            tags: vec![],
            uuid: None,
            metadata: Vec::new(),
        };
        let proto: v1::Event = event.clone().into();
        assert!(proto.metadata.is_empty());
        let round = DcbEvent::try_from(proto).unwrap();
        assert!(round.metadata.is_empty());
    }
}
