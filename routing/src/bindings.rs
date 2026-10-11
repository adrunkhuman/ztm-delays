//! Thin Python boundary. No Python handles or borrowed buffers enter the detached search.
use crate::error::Error;
use crate::network::{Network, NetworkInput, RawEnvelope, RawIncidence};
use crate::search::{Query, State, StateInput};
use pyo3::exceptions::{PyIndexError, PyOverflowError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

impl From<Error> for PyErr {
    fn from(value: Error) -> Self {
        match value {
            Error::Value(s) => PyValueError::new_err(s),
            Error::Index(s) => PyIndexError::new_err(s),
            Error::Overflow(s) => PyOverflowError::new_err(s),
            Error::Runtime(s) => PyRuntimeError::new_err(s),
        }
    }
}

#[pyclass(frozen, weakref, module = "ztm_routing._native")]
struct PreparedNet {
    inner: Arc<Network>,
}

#[pymethods]
impl PreparedNet {
    #[new]
    #[allow(clippy::too_many_arguments)] // Copy all numeric columns in one preparation call.
    fn new(
        nstops: usize,
        late_base: Vec<i32>,
        expected: Vec<i32>,
        depart: Vec<i32>,
        range_ids: Vec<i32>,
        trip_rows: Vec<i32>,
        trip_pattern: Vec<i32>,
        cumulative: Vec<f64>,
        patterns: Vec<Vec<i32>>,
        pattern_alights: Vec<Vec<i32>>,
        pattern_prefix: Vec<Vec<f64>>,
        incidence: Vec<Vec<RawIncidence>>,
        footpaths: Vec<Vec<(i32, i64)>>,
        envelopes: Vec<RawEnvelope>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(Network::new(NetworkInput {
                nstops,
                late_base,
                expected,
                depart,
                range_ids,
                trip_rows,
                trip_pattern,
                cumulative,
                patterns,
                pattern_alights,
                pattern_prefix,
                incidence,
                footpaths,
                envelopes,
            })?),
        })
    }
    fn payload_bytes(&self) -> usize {
        self.inner.payload_bytes()
    }
    fn late(&self, board: i32, alight: i32) -> PyResult<i64> {
        Ok(self.inner.late(board, alight)?)
    }
}

#[pyclass(frozen, weakref, module = "ztm_routing._native")]
struct PreparedQuery {
    inner: Arc<Query>,
}

#[pymethods]
impl PreparedQuery {
    #[new]
    fn new(network: &PreparedNet, to_target: Vec<f64>, permission_words: usize) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(Query::new(
                Arc::clone(&network.inner),
                to_target,
                permission_words,
            )?),
        })
    }
    // Atomic read: never wait for a search's mutex while attached to Python.
    fn peak_permission_words(&self) -> usize {
        self.inner.peak_permission_words()
    }
}

#[pyclass(weakref, module = "ztm_routing._native")]
struct NativeState {
    inner: State,
}

#[pymethods]
impl NativeState {
    #[new]
    fn new(
        query: &PreparedQuery,
        origins: Vec<i32>,
        access: Vec<(i32, i64)>,
        origin_walks: Vec<(i32, i32, i64)>,
        targets: Vec<(i32, i64)>,
        has_egress: bool,
        horizon_seconds: i64,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: State::new(
                Arc::clone(&query.inner),
                StateInput {
                    origins,
                    access,
                    origin_walks,
                    targets,
                    has_egress,
                    horizon_seconds,
                },
            )?,
        })
    }

    #[pyo3(signature = (after, boarding=None))]
    fn run<'py>(
        &mut self,
        py: Python<'py>,
        after: &Bound<'py, PyAny>,
        boarding: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Extract inside the failure boundary: even a huge Python int or invalid sequence
        // makes the window unusable, matching a failed partially advanced departure.
        let result = (|| {
            let after = after.extract::<i64>()?;
            let boarding = boarding.map(|b| b.extract::<Vec<i32>>()).transpose()?;
            let paths = py
                .detach(|| self.inner.run(after, boarding.as_deref()))
                .map_err(PyErr::from)?;
            paths.into_pyobject(py)
        })();
        if result.is_err() {
            self.inner.invalidate();
        }
        result
    }
    fn invalidate(&mut self) {
        self.inner.invalidate();
    }
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let result = PyDict::new(py);
        for (key, value) in self.inner.stats() {
            result.set_item(key, value)?;
        }
        Ok(result)
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PreparedNet>()?;
    m.add_class::<PreparedQuery>()?;
    m.add_class::<NativeState>()?;
    Ok(())
}
