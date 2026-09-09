# S3 access-log test fixtures

Small samples of **real** S3 server access logs from
`gs://pudl-s3-logs.catalyst.coop`, scrubbed with [`scrub.py`](./scrub.py):

- remote client IPs are mapped into the RFC 5737 documentation range
  (`198.51.100.0/24`), deterministically so request cardinality is preserved;
- the bucket-owner canonical id is replaced with a fixed placeholder.

Everything else (operations, object keys, request URIs, user agents, byte
counts, timings, status codes, and the number of columns) is untouched, so the
fixtures exercise real-world parsing quirks.

| Directory | What it covers |
| --- | --- |
| `normal_v1/` | Pre-`aws_region` schema (26 raw fields → 27 after the timestamp split); multiple files, one partition day (`2024-06-15`). |
| `normal_v2/` | Post-Feb-2026 schema with the trailing `aws_region` field (27 raw fields → 28); one partition day (`2026-09-01`). |
| `ragged/` | A single file whose column count changes partway through, as happened across the `2026-02-25` partition. |
| `empty/` | A zero-byte log object. |

Filenames mimic real blob names (`YYYY-MM-DD-HH-MM-SS-<hex>`) so tests can filter
them by partition date the way `S3Extractor.filter_blobs` does.

## Regenerating

```
gcloud storage cp gs://pudl-s3-logs.catalyst.coop/<blob> - \
  | python tests/data/s3/scrub.py --owner <real-bucket-owner-id> \
  >> tests/data/s3/<dir>/<blob>
```
