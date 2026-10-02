# S3 SDK benchmarks

This is a separate module. Its `replace` directive uses the parent checkout,
including uncommitted changes. Official AWS SDK for Go v2 dependencies stay here.

Every benchmark has an SDK reference callback. Both clients use the same local
mock, fixtures, object keys, payloads, and byte ranges. Signing compares the two
signers directly without sending a request. Static fake credentials and an
explicit endpoint keep all HTTP requests local.

Run the full suite or select a prefix:

```sh
go -C bench run .
go -C bench run . -bench s3/ -n
```

The table shows the library's `time/op`, `ops/s`, and `allocs/op`; `vs ref` compares
its timing with the SDK in the same run. `vs prev` compares the saved library
baseline. `-n` preserves `bench.gob`. The runner alternates which client runs
first across 100 samples of 10 ms. Treat an uncertain comparison as inconclusive.

The standard Go benchmarks also run `s3` and `sdk` subtests for writes, listings,
multipart upload, and composition. These report each client's time and allocation
counts, including `B/op`:

```sh
go -C bench test -run '^$' -bench . -benchmem -count=5
go -C bench test -race ./...
```

The SDK disables automatic retries and optional request/response checksums.
PUT, upload-part, and multipart-completion requests use unsigned payloads to
match the library, even over the mock's plain HTTP connection. Bodyless requests
retain the empty SHA-256 hash. This compares matched policies rather than the
SDK's default HTTP body hashing.

Multipart upload uses two concurrent 5 MiB parts, then a 128 KiB tail, then
completion. Compose uses create, one conditional ranged copy, and completion.
Small upload compares `WriteFrom` with SDK `PutObject`; the library's extra
ReaderAt-to-buffer copy is part of that measurement. Listing compares filesystem
entries with SDK object metadata, including the library's wrapping and sorting.
Glob applies the same pattern to listed metadata without per-object GET or HEAD.
Delete includes the same mock fixture reset in both timed callbacks.

Tests compare presigned URLs, object bytes, ETags, ranges, conditional rejection,
paginated listings, and multipart request sequences. A smoke test executes every
benchmark and its reference. Measurements include client and mock-server work;
they do not measure live AWS latency or prove the mock matches every S3 behavior.
