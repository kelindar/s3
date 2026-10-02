# SDK differential fuzzing

This module compares the local library with the official AWS SDK for Go v2.
Its `replace` directive tests the parent checkout, including uncommitted fixes.
The SDK dependencies stay in this module.
Each fuzz worker reuses its servers and clients and resets object state between
inputs, so longer runs do not consume a new socket port for every input.

| Target | Compared behavior |
| --- | --- |
| `FuzzSigning` | SigV4 authorization headers using the SDK signer at the library's returned signing time, including escaping, query values, header whitespace, session tokens, and unsigned payloads. |
| `FuzzObject` | Cross-client PUT and GET, HEAD metadata, conditional ranges, and deletion. |
| `FuzzConditions` | Create-only, matching-ETag, and stale-ETag writes on present and missing objects, including stored bytes after rejection. |
| `FuzzListing` | File and directory paths, sizes, ETags, ordering, URL encoding, and opaque continuation tokens across pages. |

All requests use local servers and fixed fake credentials. The tests do not
load ambient credentials or access AWS. The SDK client disables automatic
retries and optional checksums so those policies do not affect comparisons.

Object cases cover canonical filesystem paths up to 1,024 bytes and payloads
up to 64 KiB. Directory markers, noncanonical paths, and zero-width HTTP ranges
are outside the shared contract. Signing also exercises raw object paths.
Signing headers exclude invalid HTTP control characters.

Listings use an independent XML fixture because the existing object mock
does not implement `encoding-type=url`. The SDK returns encoded keys and
prefixes, which the test decodes before comparing filesystem entries. SDK
precondition errors are compared with this library's `applied=false` result.
These tests compare clients; they do not validate the mock against live S3.

Run the seed corpus, including any saved failures, from the repository root:

```sh
go -C fuzz test -race ./...
```

Run one target at a time. Go saves minimized failures in
`fuzz/testdata/fuzz/<target>/` and replays them during normal tests.

```sh
go -C fuzz test -run '^$' -fuzz '^FuzzSigning$' -fuzztime=1m -parallel=2
go -C fuzz test -run '^$' -fuzz '^FuzzObject$' -fuzztime=1m -parallel=2
go -C fuzz test -run '^$' -fuzz '^FuzzConditions$' -fuzztime=1m -parallel=2
go -C fuzz test -run '^$' -fuzz '^FuzzListing$' -fuzztime=1m -parallel=2
```

CI runs the seed corpus with the race detector. Mutation runs are explicit so
regular CI duration stays predictable.

The saved signing regression covers an embedded header tab. The SDK preserves
that tab while collapsing consecutive spaces, consistent with the
[AWS header canonicalization rules](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html).
