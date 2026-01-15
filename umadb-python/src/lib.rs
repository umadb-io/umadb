use pyo3::exceptions::{
    PyException, PyKeyboardInterrupt, PyPermissionError, PyRuntimeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::wrap_pyfunction;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::*;
use std::sync::{Arc, Mutex};
use umadb_client::{SyncUmaDBClient, UmaDBClient, trigger_cancel};
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreSync, DCBQuery, DCBQueryItem,
    DCBSequencedEvent, TrackingInfo,
};
use uuid::Uuid;

use pyo3::create_exception;

create_exception!(umadb, IntegrityError, PyValueError);
create_exception!(umadb, TransportError, PyRuntimeError);
create_exception!(umadb, CorruptionError, PyRuntimeError);
create_exception!(umadb, AuthenticationError, PyPermissionError);

/// Convert DCBError to Python exception
fn dcb_error_to_py_err(err: DCBError) -> PyErr {
    match err {
        DCBError::InvalidArgument(msg) => PyValueError::new_err(msg),
        DCBError::IntegrityError(msg) => IntegrityError::new_err(msg),
        DCBError::TransportError(msg) => TransportError::new_err(msg),
        DCBError::Corruption(msg) => CorruptionError::new_err(msg),
        DCBError::CancelledByUser() => PyKeyboardInterrupt::new_err(()),
        DCBError::AuthenticationError(msg) => AuthenticationError::new_err(msg),
        other => PyException::new_err(format!("{}", other)),
    }
}

/// Python wrapper for DCBEvent
#[gen_stub_pyclass]
#[pyclass(name = "Event")]
#[derive(Clone)]
pub struct PyEvent {
    inner: DCBEvent,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyEvent {
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

        Ok(PyEvent {
            inner: DCBEvent {
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

/// Python wrapper for DCBSequencedEvent
#[gen_stub_pyclass]
#[pyclass(name = "SequencedEvent")]
pub struct PySequencedEvent {
    inner: DCBSequencedEvent,
}

#[gen_stub_pyclass]
#[pyclass(name = "TrackingInfo")]
#[derive(Clone)]
pub struct PyTrackingInfo {
    inner: TrackingInfo,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTrackingInfo {
    #[new]
    fn new(source: String, position: u64) -> Self {
        PyTrackingInfo {
            inner: TrackingInfo { source, position },
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

#[gen_stub_pymethods]
#[pymethods]
impl PySequencedEvent {
    #[getter]
    fn event(&self) -> PyEvent {
        PyEvent {
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

/// Python wrapper for DCBQueryItem
#[gen_stub_pyclass]
#[pyclass(name = "QueryItem")]
#[derive(Clone)]
pub struct PyQueryItem {
    inner: DCBQueryItem,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQueryItem {
    #[new]
    #[pyo3(signature = (types=None, tags=None))]
    fn new(types: Option<Vec<String>>, tags: Option<Vec<String>>) -> Self {
        PyQueryItem {
            inner: DCBQueryItem {
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

/// Python wrapper for DCBQuery
#[gen_stub_pyclass]
#[pyclass(name = "Query")]
#[derive(Clone)]
pub struct PyQuery {
    inner: DCBQuery,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQuery {
    #[new]
    #[pyo3(signature = (items=None))]
    fn new(items: Option<Vec<PyQueryItem>>) -> Self {
        let query_items = items
            .unwrap_or_default()
            .into_iter()
            .map(|item| item.inner)
            .collect();

        PyQuery {
            inner: DCBQuery { items: query_items },
        }
    }

    fn __repr__(&self) -> String {
        format!("Query(items=<{} items>)", self.inner.items.len())
    }
}

/// Python wrapper for DCBAppendCondition
#[gen_stub_pyclass]
#[pyclass(name = "AppendCondition")]
#[derive(Clone)]
pub struct PyAppendCondition {
    inner: DCBAppendCondition,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAppendCondition {
    #[new]
    #[pyo3(signature = (fail_if_events_match, after=None))]
    fn new(fail_if_events_match: PyQuery, after: Option<u64>) -> Self {
        PyAppendCondition {
            inner: DCBAppendCondition {
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
#[pyclass(name = "ReadResponse")]
pub struct PyReadResponse {
    inner: Arc<Mutex<Box<dyn umadb_dcb::DCBReadResponseSync + Send + 'static>>>,
}

#[gen_stub_pymethods()]
#[pymethods]
impl PyReadResponse {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(slf: PyRefMut<Self>, py: Python<'_>) -> Option<PyResult<PySequencedEvent>> {
        // Clone the Arc and drop the PyRefMut before releasing the GIL so the closure doesn't capture non-Send data
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || {
            // Release the GIL while we potentially block waiting for the next batch/event
            inner.lock().unwrap().next()
        });
        match result {
            Some(Ok(event)) => Some(Ok(PySequencedEvent { inner: event })),
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
    ) -> PyResult<(Vec<PySequencedEvent>, Option<u64>)> {
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || inner.lock().unwrap().collect_with_head());
        match result {
            Ok((events, head)) => {
                let py_events: Vec<PySequencedEvent> = events
                    .into_iter()
                    .map(|e| PySequencedEvent { inner: e })
                    .collect();
                Ok((py_events, head))
            }
            Err(err) => Err(dcb_error_to_py_err(err)),
        }
    }

    /// Returns the next batch of events for this read. If there are no more events, returns an empty list.
    fn next_batch(slf: PyRefMut<Self>, py: Python<'_>) -> PyResult<Vec<PySequencedEvent>> {
        let inner = slf.inner.clone();
        drop(slf);
        let result = py.detach(move || inner.lock().unwrap().next_batch());
        match result {
            Ok(batch) => Ok(batch
                .into_iter()
                .map(|e| PySequencedEvent { inner: e })
                .collect()),
            Err(err) => Err(dcb_error_to_py_err(err)),
        }
    }
}

/// Python wrapper for the synchronous UmaDB client
#[gen_stub_pyclass]
#[pyclass(name = "Client")]
pub struct PyUmaDBClient {
    inner: Arc<SyncUmaDBClient>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyUmaDBClient {
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
        let client = UmaDBClient::new(url);
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

        Ok(PyUmaDBClient {
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
        query: Option<PyQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool,
    ) -> PyResult<PyReadResponse> {
        let query_inner = query.map(|q| q.inner);
        let inner = self.inner.clone();
        let response_iter = py
            .detach(move || inner.read(query_inner, start, backwards, limit, subscribe))
            .map_err(dcb_error_to_py_err)?;

        Ok(PyReadResponse {
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
        events: Vec<PyEvent>,
        condition: Option<PyAppendCondition>,
        tracking_info: Option<PyTrackingInfo>,
    ) -> PyResult<u64> {
        let dcb_events: Vec<DCBEvent> = events.into_iter().map(|e| e.inner).collect();
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
    trigger_cancel();
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
define_stub_info_gatherer!(generate_umadb_pyi_stubs);

#[pyfunction]
#[pyo3(text_signature = "()")]
// Generate stubs
fn _generate_umadb_pyi_stubs() {
    let stub = generate_umadb_pyi_stubs().expect("couldn't derive stubs");
    stub.generate().expect("couldn't generate stubs");
}

/// UmaDB Python client module
#[pymodule]
fn _umadb(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyUmaDBClient>()?;
    m.add_class::<PyEvent>()?;
    m.add_class::<PySequencedEvent>()?;
    m.add_class::<PyReadResponse>()?;
    m.add_class::<PyQuery>()?;
    m.add_class::<PyQueryItem>()?;
    m.add_class::<PyAppendCondition>()?;
    m.add_class::<PyTrackingInfo>()?;
    m.add_function(wrap_pyfunction!(trigger_cancel_from_python, m)?)?;
    m.add_function(wrap_pyfunction!(trigger_cancel_from_python2, m)?)?;
    m.add_function(wrap_pyfunction!(_generate_umadb_pyi_stubs, m)?)?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;
    m.add("TransportError", py.get_type::<TransportError>())?;
    m.add("CorruptionError", py.get_type::<CorruptionError>())?;
    m.add("AuthenticationError", py.get_type::<AuthenticationError>())?;
    Ok(())
}
