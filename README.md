longnamefs-rs
=============

Rust + FUSE3 rewrite of `longnamefs`, compatible with the original C backend layout.

- Uses the same path mapping: each path segment is SHA256 hashed and truncated to the first 16 bytes (32 hex characters).
- Stores the original name alongside data in a namefile named `<hash>n`, containing the raw bytes of the original name.
- Safe to alternate with the C version on the same backend directory.

Requirements
------------

- libfuse3 available on the system.
- Rust toolchain (edition 2024). The binary uses Tokio runtime via the `fuse3` crate.

Build
-----

```bash
cargo build --release
```

Usage
-----

```bash
longnamefs-rs --backend /path/to/backend /path/to/mountpoint \
  [--allow-other] [--nonempty] [--dir-cache-ttl-ms 1000 | --no-dir-cache]
```

- `--backend` (required): directory where hashed entries and namefiles are stored.
- `--allow-other`: pass `allow_other` to FUSE.
- `--nonempty`: allow mounting on a non-empty mountpoint.
- `--dir-cache-ttl-ms`: per-directory readdir cache TTL in milliseconds (default 1000).
- `--no-dir-cache`: disable directory cache entirely (useful for debugging correctness).

Behavior
--------

- Read/write, create, rename, link, symlink, mkdir/mknod, truncate, chmod/chown, utimens, and statfs mirror the C implementation.
- Directory listings reconstruct original names by reading corresponding namefiles and ignore stray files not matching `<hash>n`; entries are cached per-directory for the configured TTL (default 1s) and invalidated on mutating ops to cut repeated backend I/O.
- Operations on `/` interact directly with the backend directory (chmod/chown/utimens supported; truncate disallowed).
- Extended attributes (get/set/list/remove) are forwarded to the backend objects; `position` must be zero on Linux.
- `readdirplus` returns names with attributes; `flush`/`fsyncdir` are implemented; `poll` is accepted (returns no ready events).
