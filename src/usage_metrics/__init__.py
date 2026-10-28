"""Module containing dagster tools for cleaning PUDL usage metrics."""

import warnings

# google-auth warns when it imports with grpcio < 1.83.0, because in April 2027 it
# will start requiring a version with post-quantum cryptography support. There is
# nothing to do about it until a newer grpcio is available, and it is noise on every
# run, so ignore just this message. The pattern allows a prefix, so the variants
# other google-* libraries emit are caught as well. This has to happen before
# anything below imports google.auth. Remove it once the environment has
# grpcio >= 1.83.0.
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
