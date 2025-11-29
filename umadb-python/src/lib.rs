use pyo3::exceptions::{PyException, PyKeyboardInterrupt, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::wrap_pyfunction;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::*;
use std::sync::Arc;
use umadb_client::{SyncUmaDBClient, UmaDBClient, trigger_cancel};
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreSync, DCBQuery, DCBQueryItem,
    DCBSequencedEvent,
};
use uuid::Uuid;

use pyo3::create_exception;

create_exception!(umadb, IntegrityError, PyValueError);
create_exception!(umadb, TransportError, PyRuntimeError);
create_exception!(umadb, CorruptionError, PyRuntimeError);

/// Convert DCBError to Python exception
fn dcb_error_to_py_err(err: DCBError) -> PyErr {
    match err {
        DCBError::IntegrityError(msg) => IntegrityError::new_err(msg),
        DCBError::TransportError(msg) => TransportError::new_err(msg),
        DCBError::Corruption(msg) => CorruptionError::new_err(msg),
        DCBError::CancelledByUser() => PyKeyboardInterrupt::new_err(()),
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
#[pyclass(name = "ReadResponse", unsendable)]
pub struct PyReadResponse {
    // _client: Arc<SyncUmaDBClient>,
    inner: Box<dyn Iterator<Item = Result<DCBSequencedEvent, DCBError>> + 'static>,
}

#[gen_stub_pymethods()]
#[pymethods]
impl PyReadResponse {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyResult<PySequencedEvent>> {
        match slf.inner.next() {
            Some(Ok(event)) => Some(Ok(PySequencedEvent { inner: event })),
            Some(Err(err)) => Some(Err(dcb_error_to_py_err(err))),
            None => None,
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
    ///
    /// Returns:
    ///     A connected UmaDB client
    #[new]
    #[pyo3(signature = (url, ca_path=None, batch_size=None))]
    fn new(url: String, ca_path: Option<String>, batch_size: Option<u32>) -> PyResult<Self> {
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

        let sync_client = client.connect().map_err(dcb_error_to_py_err)?;

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
        query: Option<PyQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool,
    ) -> PyResult<PyReadResponse> {
        let query_inner = query.map(|q| q.inner);

        let response_iter = self
            .inner
            .read(query_inner, start, backwards, limit, subscribe)
            .map_err(dcb_error_to_py_err)?;

        Ok(PyReadResponse {
            // _client: client_for_py,
            inner: response_iter,
        })
    }

    /// Get the current head position of the event store
    ///
    /// Returns:
    ///     Optional position (None if store is empty)
    fn head(&self) -> PyResult<Option<u64>> {
        self.inner.head().map_err(dcb_error_to_py_err)
    }

    /// Append events to the event store
    ///
    /// Args:
    ///     events: List of Event objects to append
    ///     condition: Optional AppendCondition
    ///
    /// Returns:
    ///     Position of the last appended event
    #[pyo3(signature = (events, condition=None))]
    fn append(&self, events: Vec<PyEvent>, condition: Option<PyAppendCondition>) -> PyResult<u64> {
        let dcb_events: Vec<DCBEvent> = events.into_iter().map(|e| e.inner).collect();
        let dcb_condition = condition.map(|c| c.inner);

        self.inner
            .append(dcb_events, dcb_condition)
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
    m.add_function(wrap_pyfunction!(trigger_cancel_from_python, m)?)?;
    m.add_function(wrap_pyfunction!(trigger_cancel_from_python2, m)?)?;
    m.add_function(wrap_pyfunction!(_generate_umadb_pyi_stubs, m)?)?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;
    m.add("TransportError", py.get_type::<TransportError>())?;
    m.add("CorruptionError", py.get_type::<CorruptionError>())?;
    Ok(())
}
