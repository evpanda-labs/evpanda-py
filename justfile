# Dev convenience (https://github.com/casey/just). CI runs the same
# underlying commands (see .github/workflows/). Run `just` for the full gate.

# The whole gate, in the order CI runs it.
default: lint typecheck test

install:
    pip install -e ".[dev]"

# Formats and fixes in place; CI runs the same checks with --check.
lint:
    ruff format .
    ruff check --fix .

typecheck:
    mypy

test:
    pytest

# What the release workflow uploads to PyPI.
build:
    rm -rf dist
    python -m build
    twine check --strict dist/*

# Prove the SDK still works with none of the adapter extras installed.
bare:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp="$(mktemp -d)"
    python -m venv "$tmp"
    "$tmp/bin/pip" install -q -e . pytest
    "$tmp/bin/python" -c "import evpanda, evpanda.ocpi; print(evpanda.__version__)"
    "$tmp/bin/python" -m pytest
    rm -rf "$tmp"
