# Native routing

The required C++17 kernel runs the planner's round-based search. Cython copies validated network data into native buffers and reconstructs only returned paths as Python labels. Artifact loading, endpoint resolution, departure windows and itinerary rendering remain in Python.

## Build

From `frontend/`:

```sh
uv sync --locked
uv run pytest tests/native
```

Local builds need a C++17 compiler and Python development headers. Build dependencies are pinned in `pyproject.toml`. The Docker builder installs the compiler; the runtime receives the installed wheel and `libstdc++`, not build tools. Compilation or extension-import failure is fatal. There is no runtime compilation or Python routing fallback.

## Ownership and concurrency

| State | Lifetime |
| --- | --- |
| Immutable network buffers | One prepared copy per Network identity, created under a lock and retained through weak keys. New artifacts and live-patched networks use separate copies. |
| Destination bounds and permission masks | One search, shared across its departure windows; permission storage is bounded. |
| Mutable bounds and labels | One profile/window. Labels clear after every run, including exceptions. |

The kernel retains the GIL. Independent requests have isolated mutable state and can use normal four-thread Waitress, but routing does not execute in parallel within one process. Prepared networks must not be mutated in place.

Bounds checks, signed-time overflow checks and C++ exception translation remain enabled. Compilation disables floating-point contraction and does not use fast-math. `tests/native/` contains safety/concurrency checks and an independent Python oracle used only by tests; it is not installed in the application wheel.
