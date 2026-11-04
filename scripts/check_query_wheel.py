"""Run existing query contracts against the wheel installed in this environment."""

import sys
from importlib.metadata import version
from pathlib import Path

import pytest

import arc_client

repo = Path(__file__).resolve().parents[1]
package = Path(arc_client.__file__).resolve().parent
assert repo not in package.parents, f"Imported checkout instead of wheel: {package}"
assert Path(sys.prefix).resolve() in package.parents, f"Package outside environment: {package}"
print(f"arc-tsdb-client {version('arc-tsdb-client')}: {package}", flush=True)


def check_imports():
    for name, module in tuple(sys.modules.items()):
        if name == "arc_client" or name.startswith("arc_client."):
            origin = Path(module.__file__).resolve()
            assert package in origin.parents, f"Source import during wheel tests: {name}: {origin}"


check_imports()
contracts = repo / "tests/unit/http"
tests = [
    "test_query_metadata_contracts.py::test_query_metadata_sync",
    "test_query_metadata_contracts.py::test_query_metadata_async",
    "test_arrow_metadata.py::test_capped_native_arrow_and_schema_metadata",
    "test_arrow_metadata.py::test_invalid_or_failed_arrow_never_returns_table",
    "test_arrow_metadata.py::test_legacy_without_metadata_does_not_guess_cap",
    "test_arrow_metadata.py::test_unobservable_announced_metadata_fails_closed",
    "test_arrow_metadata.py::test_main_omitted_cap_with_execution_marker_is_uncapped",
]
result = pytest.main(
    ["--import-mode=importlib", "-o", "addopts=", "-v", *[str(contracts / test) for test in tests]]
)
check_imports()
sys.exit(result)
