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
        let uuid = if proto.uuid.is_empty() {
            None
        } else {
            match Uuid::parse_str(&proto.uuid) {
                Ok(uuid) => Some(uuid),
                Err(_) => {
                    return Err(DcbError::DeserializationError(
                        "Invalid UUID in Event".to_string(),
                    ));
                }
            }
        };

        Ok(DcbEvent {
            event_type: proto.event_type,
            tags: proto.tags,
            data: proto.data,
            uuid,
        })
    }
}

impl From<DcbEvent> for v1::Event {
    fn from(event: DcbEvent) -> Self {
        v1::Event {
            event_type: event.event_type,
            tags: event.tags,
            data: event.data,
            uuid: event.uuid.map(|u| u.to_string()).unwrap_or_default(),
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
