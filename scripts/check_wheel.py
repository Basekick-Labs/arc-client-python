"""Run the unit suite against the wheel installed in this environment, not the checkout."""

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
# The whole unit suite, so a module or data file the wheel leaves out fails
# here whichever feature it belongs to. Install with [dev,all]: without pandas
# and polars their tests skip instead of running.
result = pytest.main(["--import-mode=importlib", "-o", "addopts=", "-q", str(repo / "tests/unit")])
check_imports()
sys.exit(result)
