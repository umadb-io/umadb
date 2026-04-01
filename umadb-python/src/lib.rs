use pyo3::exceptions::{
    PyException, PyKeyboardInterrupt, PyPermissionError, PyRuntimeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::wrap_pyfunction;
use pyo3_stub_gen::{define_stub_info_gatherer, create_exception, derive::*};
use std::sync::{Arc, Mutex};
use umadb_client;
use umadb_dcb;
use umadb_dcb::DcbEventStoreSync;
use uuid::Uuid;


create_exception!(umadb, IntegrityError, PyValueError);
create_exception!(umadb, TransportError, PyRuntimeError);
create_exception!(umadb, CorruptionError, PyRuntimeError);
create_exception!(umadb, AuthenticationError, PyPermissionError);

/// Convert `umadb_dcb::DcbError` to Python exception
fn dcb_error_to_py_err(err: umadb_dcb::DcbError) -> PyErr {
    match err {
        umadb_dcb::DcbError::InvalidArgument(msg) => PyValueError::new_err(msg),
        umadb_dcb::DcbError::IntegrityError(msg) => IntegrityError::new_err(msg),
        umadb_dcb::DcbError::TransportError(msg) => TransportError::new_err(msg),
        umadb_dcb::DcbError::Corruption(msg) => CorruptionError::new_err(msg),
        umadb_dcb::DcbError::CancelledByUser() => PyKeyboardInterrupt::new_err(()),
        umadb_dcb::DcbError::AuthenticationError(msg) => AuthenticationError::new_err(msg),
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
    #[pyo3(signature = (event_type, data, tags=None, uuid=None))]
    fn new(
        event_type: String,
        data: Vec<u8>,
        tags: Option<Vec<String>>,
        uuid: Option<String>,
    ) -> PyResult<Self> {
        let uuid_parsed = if let Some(uuid_str) = uuid {
            Some(
                Uuid::parse_str(&uuid_str)
                    .map_err(|e| PyValueError::new_err(format!("Invalid UUID: {}", e)))?,
            )
        } else {
            None
        };

        Ok(Event {
            inner: umadb_dcb::DcbEvent {
                event_type,
                data,
                tags: tags.unwrap_or_default(),
                uuid: uuid_parsed,
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
    fn uuid(&self) -> Option<String> {
        self.inner.uuid.map(|u| u.to_string())
    }

    fn __repr__(&self) -> String {
        format!(
            "Event(event_type='{}', data=<{} bytes>, tags={:?}, uuid={:?})",
            self.inner.event_type,
            self.inner.data.len(),
            self.inner.tags,
            self.inner.uuid.map(|u| u.to_string())
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

    #[gen_stub(override_return_type(type_repr="SequencedEvent"))]
    fn __next__(slf: PyRefMut<Self>, py: Python<'_>) -> Option<PyResult<SequencedEvent>> {
        // Clone the Arc and drop the PyRefMut before releasing the GIL so the closure doesn't capture non-Send data
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || {
            // Release the GIL while we potentially block waiting for the next batch/event
            inner.lock().unwrap().next()
        });
        match result {
            Some(Ok(event)) => Some(Ok(SequencedEvent { inner: event })),
            Some(Err(err)) => Some(Err(dcb_error_to_py_err(err))),
            None => None,
        }
    }

    /// Returns the current head position of the event store, or None if empty
    fn head(slf: PyRefMut<Self>, py: Python<'_>) -> PyResult<Option<u64>> {
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || inner.lock().unwrap().head());
        result.map_err(dcb_error_to_py_err)
    }

    /// Collects all remaining events along with the head position
    fn collect_with_head(
        slf: PyRefMut<Self>,
        py: Python<'_>,
    ) -> PyResult<(Vec<SequencedEvent>, Option<u64>)> {
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || inner.lock().unwrap().collect_with_head());
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
        let result = py.detach(move || inner.lock().unwrap().next_batch());
        match result {
            Ok(batch) => Ok(batch
                .into_iter()
                .map(|e| SequencedEvent { inner: e })
                .collect()),
            Err(err) => Err(dcb_error_to_py_err(err)),
        }
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

    #[gen_stub(override_return_type(type_repr="SequencedEvent"))]
    fn __next__(slf: PyRefMut<Self>, py: Python<'_>) -> Option<PyResult<SequencedEvent>> {
        // Clone the Arc and drop the PyRefMut before releasing the GIL so the closure doesn't capture non-Send data
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || {
            // Release the GIL while we potentially block waiting for the next batch/event
            inner.lock().unwrap().next()
        });
        match result {
            Some(Ok(event)) => Some(Ok(SequencedEvent { inner: event })),
            Some(Err(err)) => Some(Err(dcb_error_to_py_err(err))),
            None => None,
        }
    }

    /// Returns the next batch of events for this read. If there are no more events, returns an empty list.
    fn next_batch(slf: PyRefMut<Self>, py: Python<'_>) -> PyResult<Vec<SequencedEvent>> {
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || inner.lock().unwrap().next_batch());
        match result {
            Ok(batch) => Ok(batch
                .into_iter()
                .map(|e| SequencedEvent { inner: e })
                .collect()),
            Err(err) => Err(dcb_error_to_py_err(err)),
        }
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
    ///     subscribe: Whether to subscribe to new events (default: False)
    ///
    /// Returns:
    ///     List of SequencedEvent objects
    #[pyo3(signature = (query=None, start=None, backwards=false, limit=None, subscribe=false))]
    fn read(
        &self,
        py: Python<'_>,
        query: Option<Query>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool,
    ) -> PyResult<ReadResponse> {
        let query_inner = query.map(|q| q.inner);
        let inner = self.inner.clone();
        let response_iter = py
            .detach(move || inner.read(query_inner, start, backwards, limit, subscribe))
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
}

#[pyfunction]
#[pyo3(text_signature = "()")]
/// Triggers cancellation of all UmaDB subscriptions from Python.
/// Useful if you want to handle SIGINT in Python code and manually
/// notify the Rust client to stop reading.
fn trigger_cancel_from_python() {
    umadb_client::trigger_cancel();
}

#[pyfunction]
#[pyo3(text_signature = "()")]
/// Triggers cancellation of all UmaDB subscriptions from Python.
/// Useful if you want to handle SIGINT in Python code and manually
/// notify the Rust client to stop reading.
fn trigger_cancel_from_python2() {
    println!("In stub info");
}

// Define a function to gather stub information.
define_stub_info_gatherer!(stub_info);

// #[pyfunction]
// #[pyo3(text_signature = "()")]
// // Generate stubs
// fn _generate_umadb_pyi_stubs() {
//     let stub = generate_umadb_pyi_stubs().expect("couldn't derive stubs");
//     stub.generate().expect("couldn't generate stubs");
// }

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
    m.add_function(wrap_pyfunction!(trigger_cancel_from_python, m)?)?;
    m.add_function(wrap_pyfunction!(trigger_cancel_from_python2, m)?)?;
    // m.add_function(wrap_pyfunction!(_generate_umadb_pyi_stubs, m)?)?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;
    m.add("TransportError", py.get_type::<TransportError>())?;
    m.add("CorruptionError", py.get_type::<CorruptionError>())?;
    m.add("AuthenticationError", py.get_type::<AuthenticationError>())?;
    Ok(())
}
