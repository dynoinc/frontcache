use frontcache_client::CacheClient;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use pyo3_bytes::PyBytes;

#[pyclass]
struct Client {
    client: CacheClient,
}

#[pymethods]
impl Client {
    fn read_range<'py>(
        &self,
        py: Python<'py>,
        key: String,
        offset: u64,
        length: u64,
        version: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let data = client
                .read_range(&key, offset, length, &version)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok::<PyBytes, PyErr>(PyBytes::new(data))
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
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    Ok(())
}
