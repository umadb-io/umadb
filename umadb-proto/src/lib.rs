pub use crate::umadb::uma_db_service_client::UmaDbServiceClient;
pub use crate::umadb::uma_db_service_server::{UmaDbService, UmaDbServiceServer};
pub use crate::umadb::{
    AppendConditionProto, AppendRequestProto, AppendResponseProto, ErrorResponseProto, EventProto,
    HeadRequestProto, HeadResponseProto, QueryItemProto, QueryProto, ReadRequestProto,
    ReadResponseProto, SequencedEventProto,
};

use prost::Message;
use prost::bytes::Bytes;
use tonic::{Code, Status};
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBQuery, DCBQueryItem, DCBResult, DCBSequencedEvent,
};
use uuid::Uuid;

// Include the generated proto code
mod umadb {
    tonic::include_proto!("umadb");
}

// Conversion functions between proto and API types
impl TryFrom<EventProto> for DCBEvent {
    type Error = DCBError;

    fn try_from(proto: EventProto) -> DCBResult<Self> {
        let uuid = if proto.uuid.is_empty() {
            None
        } else {
            match Uuid::parse_str(&proto.uuid) {
                Ok(uuid) => Some(uuid),
                Err(_) => {
                    return Err(DCBError::DeserializationError(
                        "Invalid UUID in EventProto".to_string(),
                    ));
                }
            }
        };

        Ok(DCBEvent {
            event_type: proto.event_type,
            tags: proto.tags,
            data: proto.data,
            uuid,
        })
    }
}

impl From<DCBEvent> for EventProto {
    fn from(event: DCBEvent) -> Self {
        EventProto {
            event_type: event.event_type,
            tags: event.tags,
            data: event.data,
            uuid: event.uuid.map(|u| u.to_string()).unwrap_or_default(),
        }
    }
}

impl From<QueryItemProto> for DCBQueryItem {
    fn from(proto: QueryItemProto) -> Self {
        DCBQueryItem {
            types: proto.types,
            tags: proto.tags,
        }
    }
}

impl From<DCBQueryItem> for QueryItemProto {
    fn from(item: DCBQueryItem) -> Self {
        QueryItemProto {
            types: item.types,
            tags: item.tags,
        }
    }
}

impl From<QueryProto> for DCBQuery {
    fn from(proto: QueryProto) -> Self {
        DCBQuery {
            items: proto.items.into_iter().map(|item| item.into()).collect(),
        }
    }
}

impl From<DCBQuery> for QueryProto {
    fn from(query: DCBQuery) -> Self {
        QueryProto {
            items: query.items.into_iter().map(|item| item.into()).collect(),
        }
    }
}

impl From<AppendConditionProto> for DCBAppendCondition {
    fn from(proto: AppendConditionProto) -> Self {
        DCBAppendCondition {
            fail_if_events_match: proto
                .fail_if_events_match
                .map_or_else(DCBQuery::default, |q| q.into()),
            after: proto.after,
        }
    }
}

impl From<DCBSequencedEvent> for SequencedEventProto {
    fn from(event: DCBSequencedEvent) -> Self {
        SequencedEventProto {
            position: event.position,
            event: Some(event.event.into()),
        }
    }
}

// Helper: map DCBError -> tonic::Status with structured details
pub fn status_from_dcb_error(e: &DCBError) -> Status {
    let (code, error_type) = match e {
        DCBError::IntegrityError(_) => (
            Code::FailedPrecondition,
            umadb::error_response_proto::ErrorType::Integrity as i32,
        ),
        DCBError::Corruption(_)
        | DCBError::DatabaseCorrupted(_)
        | DCBError::DeserializationError(_) => (
            Code::DataLoss,
            umadb::error_response_proto::ErrorType::Corruption as i32,
        ),
        DCBError::SerializationError(_) => (
            Code::InvalidArgument,
            umadb::error_response_proto::ErrorType::Serialization as i32,
        ),
        DCBError::InternalError(_) => (
            Code::Internal,
            umadb::error_response_proto::ErrorType::Internal as i32,
        ),
        _ => (
            Code::Internal,
            umadb::error_response_proto::ErrorType::Io as i32,
        ),
    };
    let msg = e.to_string();
    let detail = ErrorResponseProto {
        message: msg.clone(),
        error_type,
    };
    let bytes = detail.encode_to_vec();
    Status::with_details(code, msg, Bytes::from(bytes))
}

// Helper: map tonic::Status -> DCBError by decoding details
pub fn dcb_error_from_status(status: Status) -> DCBError {
    let details = status.details();
    // Try to decode ErrorResponseProto directly from details
    if !details.is_empty()
        && let Ok(err) = ErrorResponseProto::decode(details)
    {
        return match err.error_type {
            x if x == umadb::error_response_proto::ErrorType::Integrity as i32 => {
                DCBError::IntegrityError(err.message)
            }
            x if x == umadb::error_response_proto::ErrorType::Corruption as i32 => {
                DCBError::Corruption(err.message)
            }
            x if x == umadb::error_response_proto::ErrorType::Serialization as i32 => {
                DCBError::SerializationError(err.message)
            }
            x if x == umadb::error_response_proto::ErrorType::Internal as i32 => {
                DCBError::InternalError(err.message)
            }
            _ => DCBError::Io(std::io::Error::other(err.message)),
        };
    }
    // Fallback: infer from gRPC code
    match status.code() {
        Code::FailedPrecondition => DCBError::IntegrityError(status.message().to_string()),
        Code::DataLoss => DCBError::Corruption(status.message().to_string()),
        Code::InvalidArgument => DCBError::SerializationError(status.message().to_string()),
        Code::Internal => DCBError::InternalError(status.message().to_string()),
        _ => DCBError::Io(std::io::Error::other(format!("gRPC error: {}", status))),
    }
}
