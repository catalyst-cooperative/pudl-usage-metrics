"""Module containing dagster tools for cleaning PUDL usage metrics."""

import warnings

# The google-* libraries emit a FutureWarning at import (from google.api_core and
# google.auth.transport.grpc) because the pinned grpcio (1.78.1, the newest with
# a Python 3.14 conda-forge build) predates post-quantum-crypto support. Nothing
# to act on until the Oct 2026 deadline; filter it here so it doesn't clutter CLI
# output. Mirrored in pyproject.toml's pytest filterwarnings for test order
# independence. Drop both once grpcio >= 1.83 is installable.
warnings.filterwarnings(
    "ignore",
    message=r".*grpcio < 1\.83\.0 does not support Post-Quantum Cryptography",
    category=FutureWarning,
)

from . import (  # noqa: E402
    core,
    out,
    raw,
)
