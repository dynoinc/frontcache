use frontcache_client::CacheClient;
use futures::StreamExt;
use pyo3::{
    exceptions::{PyRuntimeError, PyStopAsyncIteration},
    prelude::*,
};
use pyo3_bytes::PyBytes;
use std::sync::Arc;
use tokio::sync::Mutex;

#[pyclass]
struct Client {
    client: CacheClient,
}

#[pyclass]
struct ReadStream {
    stream: Arc<Mutex<frontcache_client::ByteStream>>,
}

#[pymethods]
impl ReadStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = stream.lock().await;
            match guard.next().await {
                Some(Ok(data)) => Ok::<PyBytes, PyErr>(PyBytes::new(data)),
                Some(Err(e)) => Err(PyRuntimeError::new_err(e.to_string())),
                None => Err(PyStopAsyncIteration::new_err("")),
            }
        })
    }
}

#[pymethods]
impl Client {
    fn stream_range(
        &self,
        key: String,
        offset: u64,
        length: u64,
        version: Option<String>,
    ) -> PyResult<ReadStream> {
        let stream = self
            .client
            .stream_range(&key, offset, length, version.as_deref())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(ReadStream {
            stream: Arc::new(Mutex::new(stream)),
        })
    }
}

#[pyfunction]
fn connect<'py>(py: Python<'py>, router_addr: String) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let cache_client = CacheClient::new(router_addr)
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok::<Client, PyErr>(Client {
            client: cache_client,
        })
    })
}

#[pymodule]
fn frontcache(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<ReadStream>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    Ok(())
}
