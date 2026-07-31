use std::sync::Arc;
use tokio::sync::oneshot;
use umadb_dcb::{DcbAppendCondition, DcbEvent, DcbResult, TrackingInfo};

// Message types for communication between the gRPC server and the writer thread
pub enum WriterThreadRequest {
    Append {
        events: Vec<DcbEvent>,
        condition: Option<DcbAppendCondition>,
        tracking_info: Option<TrackingInfo>,
        response_tx: oneshot::Sender<DcbResult<u64>>,
        cancel: Option<Arc<std::sync::atomic::AtomicBool>>,
    },
    Shutdown,
}

