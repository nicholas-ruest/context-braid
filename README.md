# Context Braid

Context Braid is a small Rust CLI and library for multi-agent teams that share mutable context. It records sourced claims in an append-only JSONL ledger, links every event with SHA-256, serializes concurrent writers with a file lock, and optionally requires a compare-and-swap head. Compact snapshots retain every unresolved claim—including opposing claims—instead of silently turning one writer's summary into consensus.

The project is informed by recent work on context-learning systems and their open problems around destructive context edits and multi-writer contention. It is an independent implementation and has no model or hosted-service dependency.

## What it does

- Appends claims with topic, stance, author, and source provenance.
- Detects edits, deletions, reordering, broken links, and invalid supersession.
- Rejects stale writers with `--expect-head`.
- Allows a claim to supersede one earlier claim on the same topic.
- Produces deterministic topic ordering and flags unresolved support/oppose conflicts.
- Exposes the same behavior as a Rust library with `unsafe` forbidden.

Context Braid is a local integrity tool, not a distributed consensus protocol. File locking coordinates cooperating processes on one filesystem; network filesystems may provide weaker lock guarantees.

## Install

Rust 1.85 or newer is supported.

```sh
cargo install --git https://github.com/nicholas-ruest/context-braid
```

Or build from source:

```sh
git clone https://github.com/nicholas-ruest/context-braid.git
cd context-braid
cargo build --release
```

## Use

Create a ledger and add independent observations:

```sh
context-braid init team-context.jsonl

context-braid append team-context.jsonl \
  --topic release-readiness \
  --stance support \
  --claim "local tests passed" \
  --source "ci://local/2026-10-26" \
  --author builder

context-braid append team-context.jsonl \
  --topic release-readiness \
  --stance oppose \
  --claim "hosted smoke test failed" \
  --source "https://ci.example/runs/42" \
  --author deployer
```

Verify the ledger and emit a compact context snapshot:

```sh
context-braid verify team-context.jsonl
context-braid snapshot team-context.jsonl --output current-context.json
```

`verify` returns the current `head`. A writer that read that version can guard against racing updates:

```sh
context-braid append team-context.jsonl \
  --topic release-readiness \
  --stance neutral \
  --claim "awaiting rerun" \
  --source "https://ci.example/runs/43" \
  --author coordinator \
  --expect-head YOUR_PREVIOUS_HEAD
```

To replace an outdated claim, pass its event id. The old event remains in the verifiable history but is omitted from the active snapshot:

```sh
context-braid append team-context.jsonl \
  --topic release-readiness \
  --stance support \
  --claim "local and hosted checks passed" \
  --source "https://ci.example/runs/44" \
  --author deployer \
  --supersedes evt-000001
```

## Verification

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

CI repeats those commands on stable Rust and checks the declared Rust 1.85 MSRV.

## Limitations

- A valid chain proves internal integrity, not that a claim or source is true.
- Timestamps use the local system clock and are not trusted ordering authorities; sequence numbers define ledger order.
- Advisory locks require every writer to cooperate.
- Snapshot files are derived views. Consumers should compare `ledger_head` with a freshly verified ledger before acting.

## License

MIT
