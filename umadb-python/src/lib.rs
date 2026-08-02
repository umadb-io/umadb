use parking_lot::Mutex;
use pyo3::exceptions::{PyException, PyPermissionError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{IntoPyObjectExt, wrap_pyfunction};
use pyo3_stub_gen::{create_exception, define_stub_info_gatherer, derive::*};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use umadb_client;
use umadb_dcb;
use umadb_dcb::{DcbError, DcbEventStoreSync};
use uuid::Uuid;

create_exception!(umadb, IntegrityError, PyValueError);
create_exception!(umadb, TransportError, PyRuntimeError);
create_exception!(umadb, CorruptionError, PyRuntimeError);
create_exception!(umadb, AuthenticationError, PyPermissionError);
create_exception!(umadb, ServerStartError, PyRuntimeError);
create_exception!(umadb, CancelledByUserError, PyException);

/// Convert `umadb_dcb::DcbError` to Python exception
fn dcb_error_to_py_err(err: DcbError) -> PyErr {
    match err {
        DcbError::InvalidArgument(msg) => PyValueError::new_err(msg),
        DcbError::IntegrityError(msg) => IntegrityError::new_err(msg),
        DcbError::TransportError(msg) => TransportError::new_err(msg),
        DcbError::Corruption(msg) => CorruptionError::new_err(msg),
        DcbError::CancelledByUser() => CancelledByUserError::new_err(()),
        DcbError::AuthenticationError(msg) => AuthenticationError::new_err(msg),
        other => PyException::new_err(format!("{}", other)),
    }
}

/// Python wrapper for `DcbEvent`
#[gen_stub_pyclass]
#[derive(Clone)]
#[pyclass(from_py_object)]
pub struct Event {
    inner: umadb_dcb::DcbEvent,
}

#[gen_stub_pymethods]
#[pymethods]
impl Event {
    #[new]
    #[pyo3(signature = (event_type, data, tags=None, uuid=None, metadata=None))]
    fn new(
        event_type: String,
        data: Vec<u8>,
        tags: Option<Vec<String>>,
        #[gen_stub(override_type(type_repr = "typing.Optional[_uuid.UUID]"))] uuid: Option<Uuid>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        Ok(Event {
            inner: umadb_dcb::DcbEvent {
                event_type,
                data,
                tags: tags.unwrap_or_default(),
                uuid,
                metadata: metadata.unwrap_or_default().into_iter().collect(),
            },
        })
    }

    #[getter]
    fn event_type(&self) -> String {
        self.inner.event_type.clone()
    }

    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.data)
    }

    #[getter]
    fn tags(&self) -> Vec<String> {
        self.inner.tags.clone()
    }

    #[getter]
    #[gen_stub(override_return_type(type_repr = "typing.Optional[_uuid.UUID]"))]
    fn uuid(&self) -> Option<Uuid> {
        self.inner.uuid
    }

    #[getter]
    fn metadata(&self) -> HashMap<String, String> {
        self.inner.metadata.iter().cloned().collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "Event(event_type='{}', data=<{} bytes>, tags={:?}, uuid={:?}, metadata={:?})",
            self.inner.event_type,
            self.inner.data.len(),
            self.inner.tags,
            self.inner.uuid,
            self.inner.metadata
        )
    }
}

/// Python wrapper for `umadb_dcb::DcbSequencedEvent`
#[gen_stub_pyclass]
#[pyclass]
pub struct SequencedEvent {
    inner: umadb_dcb::DcbSequencedEvent,
}

#[gen_stub_pymethods]
#[pymethods]
impl SequencedEvent {
    #[getter]
    fn event(&self) -> Event {
        Event {
            inner: self.inner.event.clone(),
        }
    }

    #[getter]
    fn position(&self) -> u64 {
        self.inner.position
    }

    #[getter]
    fn tracking_info(&self) -> Option<TrackingInfo> {
        self.inner
            .tracking_info
            .clone()
            .map(|inner| TrackingInfo { inner })
    }

    fn __repr__(&self) -> String {
        format!(
            "SequencedEvent(position={}, event_type='{}')",
            self.inner.position, self.inner.event.event_type
        )
    }
}

#[gen_stub_pyclass]
#[derive(Clone)]
#[pyclass(from_py_object)]
pub struct TrackingInfo {
    inner: umadb_dcb::TrackingInfo,
}

#[gen_stub_pymethods]
#[pymethods]
impl TrackingInfo {
    #[new]
    fn new(source: String, position: u64) -> Self {
        TrackingInfo {
            inner: umadb_dcb::TrackingInfo { source, position },
        }
    }

    #[getter]
    fn source(&self) -> String {
        self.inner.source.clone()
    }

    #[getter]
    fn position(&self) -> u64 {
        self.inner.position
    }

    fn __repr__(&self) -> String {
        format!(
            "Tracking(source='{}', position={})",
            self.inner.source, self.inner.position
        )
    }
}

/// Python wrapper for `umadb_dcb::DcbQueryItem`
#[gen_stub_pyclass]
#[derive(Clone)]
#[pyclass(from_py_object)]
pub struct QueryItem {
    inner: umadb_dcb::DcbQueryItem,
}

#[gen_stub_pymethods]
#[pymethods]
impl QueryItem {
    #[new]
    #[pyo3(signature = (types=None, tags=None))]
    fn new(types: Option<Vec<String>>, tags: Option<Vec<String>>) -> Self {
        QueryItem {
            inner: umadb_dcb::DcbQueryItem {
                types: types.unwrap_or_default(),
                tags: tags.unwrap_or_default(),
            },
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "QueryItem(types={:?}, tags={:?})",
            self.inner.types, self.inner.tags
        )
    }
}

/// Python wrapper for `umadb_dcb::DcbQuery`
#[gen_stub_pyclass]
#[derive(Clone)]
#[pyclass(from_py_object)]
pub struct Query {
    inner: umadb_dcb::DcbQuery,
}

#[gen_stub_pymethods]
#[pymethods]
impl Query {
    #[new]
    #[pyo3(signature = (items=None))]
    fn new(items: Option<Vec<QueryItem>>) -> Self {
        let query_items = items
            .unwrap_or_default()
            .into_iter()
            .map(|item| item.inner)
            .collect();

        Query {
            inner: umadb_dcb::DcbQuery { items: query_items },
        }
    }

    fn __repr__(&self) -> String {
        format!("Query(items=<{} items>)", self.inner.items.len())
    }
}

/// Python wrapper for `umadb_dcb::DcbAppendCondition`
#[gen_stub_pyclass]
#[derive(Clone)]
#[pyclass(from_py_object)]
pub struct AppendCondition {
    inner: umadb_dcb::DcbAppendCondition,
}

#[gen_stub_pymethods]
#[pymethods]
impl AppendCondition {
    #[new]
    #[pyo3(signature = (fail_if_events_match, after=None))]
    fn new(fail_if_events_match: Query, after: Option<u64>) -> Self {
        AppendCondition {
            inner: umadb_dcb::DcbAppendCondition {
                fail_if_events_match: fail_if_events_match.inner,
                after,
            },
        }
    }

    fn __repr__(&self) -> String {
        format!("AppendCondition(after={:?})", self.inner.after)
    }
}

/// Python iterator over sequenced events
#[gen_stub_pyclass]
#[pyclass]
pub struct ReadResponse {
    inner: Arc<Mutex<Box<dyn umadb_dcb::DcbReadResponseSync + Send + 'static>>>,
}

#[gen_stub_pymethods()]
#[pymethods]
impl ReadResponse {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    #[gen_stub(override_return_type(type_repr = "SequencedEvent"))]
    fn __next__(slf: PyRefMut<Self>, py: Python<'_>) -> Option<PyResult<SequencedEvent>> {
        // Clone the Arc and drop the PyRefMut before releasing the GIL so the closure doesn't capture non-Send data
        let inner = slf.inner.clone();
        drop(slf);

        loop {
            let result = py.detach({
                let inner = inner.clone();
                move || {
                    // Call the new timeout method we just exposed on DcbReadResponseSync
                    inner.lock().next_timeout(Duration::from_millis(100))
                }
            });

            // Check Python signals every 100ms.
            if let Err(e) = py.check_signals() {
                return Some(Err(e));
            }

            match result {
                Some(Ok(event)) => return Some(Ok(SequencedEvent { inner: event })),
                // If it's a non-fatal timeout, skip the match and cycle the loop to keep waiting
                Some(Err(DcbError::Timeout())) => continue,
                Some(Err(err)) => return Some(Err(dcb_error_to_py_err(err))),
                None => return None,
            }
        }
    }

    /// Returns the current head position of the event store, or None if empty
    fn head(slf: PyRefMut<Self>, py: Python<'_>) -> PyResult<Option<u64>> {
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || inner.lock().head());
        result.map_err(dcb_error_to_py_err)
    }

    /// Collects all remaining events along with the head position
    fn collect_with_head(
        slf: PyRefMut<Self>,
        py: Python<'_>,
    ) -> PyResult<(Vec<SequencedEvent>, Option<u64>)> {
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || inner.lock().collect_with_head());
        match result {
            Ok((events, head)) => {
                let py_events: Vec<SequencedEvent> = events
                    .into_iter()
                    .map(|e| SequencedEvent { inner: e })
                    .collect();
                Ok((py_events, head))
            }
            Err(err) => Err(dcb_error_to_py_err(err)),
        }
    }

    /// Returns the next batch of events for this read. If there are no more events, returns an empty list.
    fn next_batch(slf: PyRefMut<Self>, py: Python<'_>) -> PyResult<Vec<SequencedEvent>> {
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || inner.lock().next_batch());
        match result {
            Ok(batch) => Ok(batch
                .into_iter()
                .map(|e| SequencedEvent { inner: e })
                .collect()),
            Err(err) => Err(dcb_error_to_py_err(err)),
        }
    }

    /// Ends this individual read response stream.
    ///
    /// After calling `cancel()`, iterating over this response (or calling
    /// `next_batch()`) will raise a `CancelledByUserError`. Unlike
    /// `cancel_all_stream_responses()`, this only affects this particular
    /// `ReadResponse`.
    fn cancel(slf: PyRefMut<Self>, py: Python<'_>) {
        // Delegate to the inner sync response's cancel(), which in turn signals the
        // async response to cancel. `__next__` polls with a short timeout and only
        // holds `inner`'s Mutex for that window, so acquiring the lock here does
        // not block on an in-progress read.
        let inner = slf.inner.clone();
        drop(slf);
        py.detach(move || inner.lock().cancel());
    }
}

/// Python iterator over sequenced events
#[gen_stub_pyclass]
#[pyclass]
pub struct Subscription {
    inner: Arc<Mutex<Box<dyn umadb_dcb::DcbSubscriptionSync + Send + 'static>>>,
}

#[gen_stub_pymethods()]
#[pymethods]
impl Subscription {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    #[gen_stub(override_return_type(type_repr = "SequencedEvent"))]
    fn __next__(slf: PyRefMut<Self>, py: Python<'_>) -> Option<PyResult<SequencedEvent>> {
        // Clone the Arc and drop the PyRefMut before releasing the GIL so the closure doesn't capture non-Send data
        let inner = slf.inner.clone();
        drop(slf);

        loop {
            let result = py.detach({
                let inner = inner.clone();
                move || inner.lock().next_timeout(Duration::from_millis(100))
            });

            // Check Python signals.
            if let Err(e) = py.check_signals() {
                return Some(Err(e));
            }

            match result {
                Some(Ok(event)) => return Some(Ok(SequencedEvent { inner: event })),
                Some(Err(DcbError::Timeout())) => continue,
                Some(Err(err)) => return Some(Err(dcb_error_to_py_err(err))),
                None => return None,
            }
        }
    }

    /// Returns the next batch of events for this read. If there are no more events, returns an empty list.
    fn next_batch(slf: PyRefMut<Self>, py: Python<'_>) -> PyResult<Vec<SequencedEvent>> {
        let inner = slf.inner.clone();
        drop(slf);
        loop {
            let result = py.detach({
                let inner = inner.clone();
                move || inner.lock().next_batch_timeout(Duration::from_millis(100))
            });

            // Check Python signals.
            if let Err(e) = py.check_signals() {
                return Err(e);
            }

            match result {
                Ok(batch) => {
                    return Ok(batch
                        .into_iter()
                        .map(|e| SequencedEvent { inner: e })
                        .collect());
                }
                Err(DcbError::Timeout()) => continue,
                Err(err) => return Err(dcb_error_to_py_err(err)),
            }
        }
    }

    /// Ends this individual subscription stream.
    ///
    /// After calling `cancel()`, iterating over this subscription (or calling
    /// `next_batch()`) will raise a CancelledByUserError. Unlike
    /// `cancel_all_stream_responses()`, this only affects this particular
    /// `Subscription`.
    fn cancel(slf: PyRefMut<Self>, py: Python<'_>) {
        // Delegate to the inner sync response's cancel(), which in turn signals the
        // async response to cancel. `__next__` polls with a short timeout and only
        // holds `inner`'s Mutex for that window, so acquiring the lock here does
        // not block on an in-progress read.
        let inner = slf.inner.clone();
        drop(slf);
        py.detach(move || inner.lock().cancel());
    }
}

/// Python wrapper for the synchronous UmaDB client
#[gen_stub_pyclass]
#[pyclass]
pub struct Client {
    inner: Arc<umadb_client::SyncUmaDbClient>,
}

#[gen_stub_pymethods]
#[pymethods]
impl Client {
    /// Create a new UmaDB client
    ///
    /// Args:
    ///     url: The server URL (e.g., "http://localhost:50051" or "https://server:50051")
    ///     ca_path: Optional path to CA certificate for TLS
    ///     batch_size: Optional batch size for reading events
    ///     api_key: Optional API key for authenticating clients
    ///
    ///
    /// Returns:
    ///     A connected UmaDB client
    #[new]
    #[pyo3(signature = (url, ca_path=None, batch_size=None, api_key=None))]
    fn new(
        py: Python<'_>,
        url: String,
        ca_path: Option<String>,
        batch_size: Option<u32>,
        api_key: Option<String>,
    ) -> PyResult<Self> {
        let client = umadb_client::UmaDbClient::new(url);
        let client = if let Some(ca) = ca_path {
            client.ca_path(ca)
        } else {
            client
        };
        let client = if let Some(bs) = batch_size {
            client.batch_size(bs)
        } else {
            client
        };
        let client = if let Some(k) = api_key {
            client.api_key(k)
        } else {
            client
        };

        let sync_client = py
            .detach(move || client.connect())
            .map_err(dcb_error_to_py_err)?;

        Ok(Client {
            inner: Arc::new(sync_client),
        })
    }

    /// Read events from the event store
    ///
    /// Args:
    ///     query: Optional Query to filter events
    ///     start: Optional starting position
    ///     backwards: Whether to read backwards (default: False)
    ///     limit: Optional maximum number of events to read
    ///
    /// Returns:
    ///     List of SequencedEvent objects
    #[pyo3(signature = (query=None, start=None, backwards=false, limit=None))]
    fn read(
        &self,
        py: Python<'_>,
        query: Option<Query>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
    ) -> PyResult<ReadResponse> {
        let query_inner = query.map(|q| q.inner);
        let inner = self.inner.clone();
        let response_iter = py
            .detach(move || inner.read(query_inner, start, backwards, limit))
            .map_err(dcb_error_to_py_err)?;

        Ok(ReadResponse {
            inner: Arc::new(Mutex::new(response_iter)),
        })
    }

    /// Subscribe to events from the event store
    ///
    /// This method returns optionally filtered events after
    /// an optional position. The returned iterator yields
    /// events indefinitely until canceled or the stream ends.
    ///
    /// Args:
    ///     query: Optional tags and types filter
    ///     after: Optional position filter
    ///
    /// Returns:
    ///     An iterable of SequencedEvent objects
    #[pyo3(signature = (query=None, after=None))]
    fn subscribe(
        &self,
        py: Python<'_>,
        query: Option<Query>,
        after: Option<u64>,
    ) -> PyResult<Subscription> {
        let query_inner = query.map(|q| q.inner);
        let inner = self.inner.clone();
        let response_iter = py
            .detach(move || inner.subscribe(query_inner, after))
            .map_err(dcb_error_to_py_err)?;

        Ok(Subscription {
            inner: Arc::new(Mutex::new(response_iter)),
        })
    }

    /// Get the current head position of the event store
    ///
    /// Returns:
    ///     Optional position (None if store is empty)
    fn head(&self, py: Python<'_>) -> PyResult<Option<u64>> {
        let inner = self.inner.clone();
        py.detach(move || inner.head()).map_err(dcb_error_to_py_err)
    }

    /// Append events to the event store
    ///
    /// Args:
    ///     events: List of Event objects to append
    ///     condition: Optional AppendCondition
    ///     tracking_info: Optional TrackingInfo
    ///
    /// Returns:
    ///     Position of the last appended event
    #[pyo3(signature = (events, condition=None, tracking_info=None))]
    fn append(
        &self,
        py: Python<'_>,
        events: Vec<Event>,
        condition: Option<AppendCondition>,
        tracking_info: Option<TrackingInfo>,
    ) -> PyResult<u64> {
        let dcb_events: Vec<umadb_dcb::DcbEvent> = events.into_iter().map(|e| e.inner).collect();
        let dcb_condition = condition.map(|c| c.inner);
        let dcb_tracking_info = tracking_info.map(|t| t.inner);
        let inner = self.inner.clone();
        py.detach(move || inner.append(dcb_events, dcb_condition, dcb_tracking_info))
            .map_err(dcb_error_to_py_err)
    }

    /// Get the recorded tracking position for a source
    ///
    /// Args:
    ///     source: The tracking source identifier
    ///
    /// Returns:
    ///     Optional position last recorded for this source (None if not set)
    fn get_tracking_info(&self, py: Python<'_>, source: String) -> PyResult<Option<u64>> {
        let inner = self.inner.clone();
        py.detach(move || inner.get_tracking_info(&source))
            .map_err(dcb_error_to_py_err)
    }

    fn __repr__(&self) -> String {
        "Client(connected)".to_string()
    }

    // --- CONTEXT MANAGER IMPLEMENTATION ---

    /// Context manager enter - will call close on exit.
    fn __enter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Convert the structural reference into a generic, un-borrowed Bound Python pointer.
        // This drops the exclusive Rust borrow lock immediately, allowing thread sharing!
        slf.into_bound_py_any(py)
    }

    /// Context manager exit - calls close.
    #[pyo3(signature = (exc_type, exc_val, exc_tb, /))]
    #[allow(unused_variables)]
    fn __exit__(
        &mut self,
        exc_type: &Bound<'_, PyAny>,
        exc_val: &Bound<'_, PyAny>,
        exc_tb: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        // Close the streams.
        self.close()?;

        // Return false to ensure any internal exceptions are propagated up to Python normally
        Ok(false)
    }

    /// Stops all active streaming responses opened by this client.
    fn close(&self) -> PyResult<()> {
        // The active-stream registry lives in the underlying Rust client.
        self.inner.close();
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Cancel all background streams before the core network channel is destroyed.
        // (Also covered by the Rust client's own Drop, but this cancels streams promptly
        // even if the client Arc outlives this wrapper.)
        self.inner.close();
    }
}

#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(text_signature = "()")]
/// Client-side cancellation of all active read and subscription response streams.
///
/// This only affects streams opened by this Python client process, such as
/// `ReadResponse` values returned by `Client.read()` and `Subscription`
/// values returned by `Client.subscribe()`. It does not stop, shut down,
/// or otherwise affect the UmaDB server.
fn cancel_all_stream_responses() {
    umadb_client::cancel_all_stream_responses();
}

#[cfg(not(windows))]
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (args))]
fn run_server_from_args(py: Python<'_>, args: Vec<String>) -> PyResult<()> {
    // Pass the raw arguments to our Rust clap parser
    // clap handles --help and --version natively and will gracefully exit the process if they are called.
    let options = umadb_cli::parse_cli_options(args)
        .map_err(|err| ServerStartError::new_err(err.to_string()))?;

    // Convert any runtime error directly to a ServerStartError
    let run_result = py.detach(move || {
        umadb_cli::start_server_with_cli_options(options).map_err(|err| err.to_string())
    });

    run_result.map_err(ServerStartError::new_err)
}

#[cfg(windows)]
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (args))]
fn run_server_from_args(args: Vec<String>) -> PyResult<()> {
    let _ = args;
    Err(ServerStartError::new_err(
        "Running the UmaDB server from the Python package is not supported on Windows.",
    ))
}

// Define a function to gather stub information.
define_stub_info_gatherer!(stub_info);

/// UmaDB Python client module
#[pymodule]
fn umadb(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<Event>()?;
    m.add_class::<SequencedEvent>()?;
    m.add_class::<ReadResponse>()?;
    m.add_class::<Query>()?;
    m.add_class::<QueryItem>()?;
    m.add_class::<AppendCondition>()?;
    m.add_class::<TrackingInfo>()?;
    m.add_function(wrap_pyfunction!(run_server_from_args, m)?)?;
    m.add_function(wrap_pyfunction!(cancel_all_stream_responses, m)?)?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;
    m.add("TransportError", py.get_type::<TransportError>())?;
    m.add("CorruptionError", py.get_type::<CorruptionError>())?;
    m.add("AuthenticationError", py.get_type::<AuthenticationError>())?;
    m.add("ServerStartError", py.get_type::<ServerStartError>())?;
    m.add(
        "CancelledByUserError",
        py.get_type::<CancelledByUserError>(),
    )?;
    Ok(())
}
