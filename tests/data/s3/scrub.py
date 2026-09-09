"""Scrub real S3 access-log lines for use as test fixtures.

Replaces the only PII / account-identifying values -- the remote client IP and
the bucket-owner account canonical id -- and leaves everything else (operations,
object keys, request URIs, user agents, timings, status codes, schema shape)
byte-for-byte intact. Client IPs map deterministically into the RFC 5737
documentation range (``198.51.100.0/24``) so request cardinality is preserved.

The AWS Registry of Open Data checker ARN is intentionally *not* scrubbed: it is
a well-known shared identity and is already referenced in
``usage_metrics.out.s3.REQUESTERS_IGNORE``.

Usage::

    cat real-log-file ... | python scrub.py --owner <real-bucket-owner-id> > fixture
"""

import argparse
import re
import sys
import zlib

FAKE_OWNER = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"


def fake_ip(real: str) -> str:
    """Map a real IP into the documentation range, preserving cardinality."""
    return f"198.51.100.{zlib.crc32(real.encode()) % 254 + 1}"


def scrub_line(line: str, real_owner: str) -> str:
    """Scrub one raw log line."""
    line = line.replace(real_owner, FAKE_OWNER)
    # remote_ip is the whitespace-delimited token immediately after the
    # "[dd/Mon/yyyy:hh:mm:ss +zzzz]" request timestamp.
    match = re.search(r"(\+\d{4}\]\s+)(\S+)", line)
    if match and match.group(2) != "-":
        line = line[: match.start(2)] + fake_ip(match.group(2)) + line[match.end(2) :]
    return line


def main() -> None:
    """Scrub stdin to stdout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--owner", required=True, help="Real bucket-owner canonical id."
    )
    args = parser.parse_args()
    for raw in sys.stdin:
        raw = raw.rstrip("\n")
        if raw:
            print(scrub_line(raw, args.owner))


if __name__ == "__main__":
    main()
