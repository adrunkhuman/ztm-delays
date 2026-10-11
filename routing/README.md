# ZTM routing engine

This mixed Python/Rust package exposes `PreparedNet`, `PreparedQuery`, and
`NativeState` from `ztm_routing._native`. It accepts primitive sequences, owns
copies of inputs, and has no frontend dependency. Python 3.13 or later is required.
PyO3 0.29.3 and maturin 1.15.0 are pinned; Cargo.lock pins transitive Rust dependencies.

The core uses round-based search with strict improvements, stable arrival sorting,
and insertion-order tie resolution. See `python/ztm_routing/_native.pyi` for the
boundary signatures.

Release builds use one code-generation unit and thin link-time optimization so
hot calls can be optimized across modules. Internal numeric IDs and generated
permission masks use `FxHashMap`; they are not arbitrary user-supplied text. Tie
ordering is kept in separate vectors and never depends on hash-table traversal.
Do not reuse this non-cryptographic hasher for arbitrary untrusted keys. Bounds
checks, checked time arithmetic, and ordinary IEEE floating-point operations remain
enabled; the package forbids unsafe Rust in its own source.

A network is immutable and shared through `Arc`. A query shares a bounded FIFO
permission cache across its windows. Each window owns numeric bounds that survive
decreasing departure times. Its predecessor arena is cleared on every successful
or failed run; allocated capacity may remain for reuse. Only complete returned
paths are copied out, in origin-to-destination order.

Search and returned-path extraction run detached from Python. The shared query
mutex is acquired and released only while detached. Permission high-water getters
use atomic reads. PyO3's mutable state borrow rejects overlapping state calls.
Frontend materialization failures must call `invalidate()`; a failed window
cannot be reused.

`payload_bytes()` and `state_payload_bytes` account for owned vector capacity and
hash entry payloads. They exclude hash buckets, allocator overhead, query-cache
allocation, temporary search buffers, and Python objects; they are not process RSS.

## Local checks

With Rust 1.98.0, Python 3.13, and uv installed, run from the repository root:

```sh
cargo +1.98.0 fmt --manifest-path routing/Cargo.toml --check
cargo +1.98.0 test --manifest-path routing/Cargo.toml --locked
cargo +1.98.0 clippy --manifest-path routing/Cargo.toml --locked --all-targets -- -D warnings
uv build --no-sources --out-dir routing/dist routing
```

Set `PYO3_PYTHON` to a Python 3.13+ interpreter if Cargo's interpreter discovery
needs assistance. The optional extension-module feature is enabled by maturin,
not by default, so ordinary Cargo unit tests can link normally.

After installing the wheel, `python -m unittest discover -s routing/tests -v`
checks the primitive Python boundary without frontend imports or extra test tools.
The frontend's native oracle suite is the authoritative integration parity check.
