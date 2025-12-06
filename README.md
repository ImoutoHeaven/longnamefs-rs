longnamefs-rs
=============

Rust + FUSE3 rewrite of `longnamefs`, compatible with the original C backend layout.

- Uses the same path mapping: each path segment is SHA256 hashed and truncated to the first 16 bytes (32 hex characters).
- Stores the original name alongside data in a namefile named `<hash>n`, containing the raw bytes of the original name.
- Safe to alternate with the C version on the same backend directory.

Requirements
------------
- Runtime dependencies:
  - A kernel with FUSE support enabled.
  - `libfuse3` / `fuse3` available on the system.
- Build-time (from source):
  - Rust toolchain (edition 2024; `cargo` + `rustc`).
  - C toolchain and headers.
  - `libfuse3` development headers.

Examples:

On **Debian/Ubuntu**:

```bash
sudo apt update
# To run an existing binary:
sudo apt install -y fuse3 libfuse3-3
# To build from source:
sudo apt install -y build-essential pkg-config libfuse3-dev
```

To allow non-root users to mount with `--allow-other`:

```bash
sudo adduser "$USER" fuse
echo "user_allow_other" | sudo tee -a /etc/fuse.conf
# then log out and log back in
```

Build
-----

```bash
cargo build --release
```

Usage
-----

```bash
longnamefs-rs --backend /path/to/backend /path/to/mountpoint \
  [--backend-layout v1] [--allow-other] [--nonempty] [--dir-cache-ttl-ms 1000 | --no-dir-cache] [--max-write-kb 1024] [--max-name-len 1024] [--index-sync batch] [--sync-data] [--collision-protect] [--unsafe-namefile-writes]
```

- `--backend` (required): directory where hashed entries and namefiles are stored.
- `--backend-layout` (default `v1`): `v1` is the current hash+namefile layout; `v2` enables the new xattr+index backend (incompatible with v1 backends; long-name hardlinks are rejected).
- `--allow-other`: pass `allow_other` to FUSE.
- `--nonempty`: allow mounting on a non-empty mountpoint.
- `--dir-cache-ttl-ms`: per-directory readdir cache TTL in milliseconds (default 1000); also drives the directory FD cache used by the v2 path resolver.
- `--no-dir-cache`: disable directory cache entirely (useful for debugging correctness).
- `--max-write-kb`: maximum write size advertised to FUSE in KiB (default 1024; kernel may clamp to its own maximum).
- `--max-name-len`: maximum logical segment length accepted by the v2 backend (default 1024, failures surface as `ENAMETOOLONG` or `EINVAL`).
- `--index-sync` (v2 only): `always` flush index on every mutation, `batch` (default) flushes when 128 pending changes or 5s elapsed, `off` disables background flush (index rebuild will recover).
- `--sync-data`: fdatasync data files after writes for stronger durability (at the cost of throughput).
- `--collision-protect`: probe namefiles to detect/avoid hash collisions by using suffixed `<hash>.k` entries (default off; adds I/O).
- `--unsafe-namefile-writes`: use non-transactional namefile updates (faster for many small files, but filename metadata may be lost or become inconsistent on crash).

The process stays in the foreground and runs until the filesystem is unmounted
or the process is terminated. This is intentional and makes it easy to use
with process supervisors (e.g. systemd).

Behavior
--------

- Read/write, create, rename, link, symlink, mkdir/mknod, truncate, chmod/chown, utimens, and statfs mirror the C implementation.
- Directory listings reconstruct original names by reading corresponding namefiles and ignore stray files not matching `<hash>n`; entries are cached per-directory for the configured TTL (default 1s) and invalidated on mutating ops to cut repeated backend I/O. In v2 the same TTL controls an LRU of directory FDs to reduce open/close churn when resolving deep paths.
- Operations on `/` interact directly with the backend directory (chmod/chown/utimens supported; truncate disallowed).
- Extended attributes (get/set/list/remove) are forwarded to the backend objects; `position` must be zero on Linux.
- `readdirplus` returns names with attributes; `flush`/`fsyncdir` are implemented; `poll` is accepted (returns no ready events).
- With `--backend-layout v2`, long names are mapped to internal `.__ln2_*` entries with the original bytes stored in `user.ln2.rawname`; internal names and `.ln2_index` are hidden from directory listings, and creating hardlinks for long-name entries returns `EOPNOTSUPP`.

systemd example
---------------

For long-running mounts it is recommended to let `systemd` supervise
`longnamefs-rs` rather than trying to daemonize from inside the process.

Example unit file (`/etc/systemd/system/longnamefs-rs.service`):

```ini
[Unit]
Description=longnamefs-rs FUSE filesystem
After=network.target

[Service]
Type=simple
User=youruser
Group=youruser
ExecStart=/ext/longnamefs-rs --backend /ext/raw-long /ext/mnt-long --allow-other
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now longnamefs-rs.service
```

Un-mounting is handled automatically when the service stops (Ctrl+C on
`ExecStart` or `systemctl stop`); you can also manually unmount with:

```bash
fusermount3 -u /ext/mnt-long
# or
sudo umount /ext/mnt-long
```
