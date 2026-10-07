"""Server-side concatenation of many small GCS objects with ``compose()``.

``Blob.compose`` joins up to 32 objects into one entirely inside GCS, so reducing a
partition of ~200k tiny S3 log objects to a few hundred composites moves no bytes
through the runner and needs only a few thousand cheap metadata calls.
"""

import itertools
import logging
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from google.cloud import storage

logger = logging.getLogger(__name__)

COMPOSE_FANOUT = 32
"""Maximum number of source objects per ``compose()`` call (a GCS limit)."""

COMPOSE_ROUNDS = 2
"""Number of compose rounds.

GCS documents a limit of 1024 components per composite, and composing composites
adds up their components. Two rounds of 32-way compose gives 32 * 32 = 1024
components per output object, exactly the documented limit, so no flattening
(rewriting composites back into single-component objects) is needed. A
176k-object day reduces to ~172 objects. (Validated against a real bucket: a
composite of two full 1024-component composites was accepted, so the limit is
not enforced at 1024 today -- but nothing here relies on that.)"""

COMPOSE_TMP_PREFIX = "_compacted/tmp"
"""Scratch prefix for intermediates. ``compose`` only works within one bucket, so
these live alongside the raw objects, under a prefix a partition listing never
matches. A bucket lifecycle rule deletes them; the ETL needs no delete permission."""


def _compose_group(
    bucket: storage.Bucket, group: tuple[storage.Blob, ...], dest_name: str
) -> storage.Blob:
    """Compose one group of blobs into ``dest_name``, or pass a lone blob through."""
    if len(group) == 1:
        return group[0]
    dest = bucket.blob(dest_name)
    # ``if_generation_match=0`` makes the request idempotent (the client only
    # auto-retries conditional requests) and turns an accidental overwrite into
    # a loud 412 rather than a silent replace.
    dest.compose(list(group), if_generation_match=0)
    return dest


def compose_layer(
    bucket: storage.Bucket,
    blobs: list[storage.Blob],
    dest_prefix: str,
    workers: int,
) -> list[storage.Blob]:
    """Compose ``blobs`` in name order, ``COMPOSE_FANOUT`` at a time, in parallel.

    Returns the composites in the same order as their sources.
    """
    groups = list(itertools.batched(blobs, COMPOSE_FANOUT))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(
            pool.map(
                lambda item: _compose_group(
                    bucket, item[1], f"{dest_prefix}-{item[0]:06d}"
                ),
                enumerate(groups),
            )
        )


def compose_day(
    bucket: storage.Bucket,
    source_prefix: str,
    partition_key: str,
    workers: int,
) -> tuple[list[storage.Blob], int]:
    """Reduce every ``source_prefix*`` object to ~N/1024 composites, server-side.

    Sources are taken in name order (S3 log names embed their delivery time, so
    lexical order is time order) and the byte concatenation of the returned
    blobs equals the concatenation of the original objects.

    Args:
        bucket: The bucket holding the source objects; intermediates are written
            to it as well.
        source_prefix: Name prefix selecting the source objects.
        partition_key: Used to name the scratch intermediates.
        workers: Threads for the parallel ``compose`` calls.

    Returns:
        The reduced blobs, in order, and the number of original objects.
    """
    layer = list(bucket.list_blobs(prefix=source_prefix))
    count = len(layer)
    # Unique per run: a retry never collides with (or needs to delete) leftovers.
    run_prefix = f"{COMPOSE_TMP_PREFIX}/{partition_key}/{uuid4().hex}"
    for round_number in range(COMPOSE_ROUNDS):
        if len(layer) <= 1:
            break
        logger.info(f"Compose round {round_number}: {len(layer):,} objects.")
        layer = compose_layer(bucket, layer, f"{run_prefix}/r{round_number}", workers)
    return layer, count
