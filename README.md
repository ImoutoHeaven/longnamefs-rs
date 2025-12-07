longnamefs-rs
=============

Rust + FUSE3 implementation of a “long filename shim” with two backend layouts:

- **v1 (hash + namefile)**: matches the original C `longnamefs` layout; safe to share the same backend directory with the C implementation.
- **v2 (xattr + index)**: a new backend, not compatible with v1/C; use a fresh/dedicated backend directory. Long-name hardlinks are rejected by design.

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
  [--backend-layout v1] [--allow-other] [--nonempty] \
  [--dir-cache-ttl-ms 1000 | --no-dir-cache] [--max-write-kb 1024] [--sync-data] \
  [layout-specific options...]
```

**Common options (v1 and v2)**
- `--backend` (required): backend directory on disk.
- `--backend-layout` (`v1` default): choose layout; only `v1` is compatible with the C implementation, `v2` requires its own backend.
- `--allow-other`: pass `allow_other` to FUSE.
- `--nonempty`: allow mounting on a non-empty mountpoint.
- `--dir-cache-ttl-ms`: per-directory readdir cache TTL in milliseconds (default 1000); also drives the directory FD cache used by the v2 path resolver.
- `--no-dir-cache`: disable directory cache (useful for debugging).
- `--attr-ttl-ms`: attr/entry TTL for FUSE replies (default 1000); set to 0 to disable kernel caching (recommended for v2 if you need strict visibility of newly created long names until invalidation is added).
- `--max-write-kb`: maximum write size advertised to FUSE in KiB (default 1024; kernel may clamp).
- `--sync-data`: fdatasync after writes for stronger durability (at the cost of throughput).
- The process attempts to raise `RLIMIT_NOFILE` on Unix to reduce `EMFILE` risk when many directory FDs are cached; set a generous `ulimit -n` (e.g. 65536) in production.

**v1 (hash + namefile, C-compatible)**
- `--collision-protect`: probe namefiles to detect/avoid hash collisions by using suffixed `<hash>.k` entries (default off; adds I/O).
- `--unsafe-namefile-writes`: use non-transactional namefile updates (faster for many small files, but filename metadata may be lost or become inconsistent on crash).

**v2 (xattr + index, incompatible with C/v1 backends)**
- `--max-name-len`: maximum logical segment length accepted (default 1024; returns `ENAMETOOLONG`/`EINVAL` when exceeded).
- `--index-sync`: `always` flush index on every mutation, `batch` (default) flushes when 128 pending changes or 5s elapsed, `off` disables background flush (index rebuild will recover).

The process stays in the foreground and runs until the filesystem is unmounted
or the process is terminated. This is intentional and makes it easy to use
with process supervisors (e.g. systemd).

Behavior
--------

- Read/write, create, rename, link, symlink, mkdir/mknod, truncate, chmod/chown, utimens, and statfs mirror the C implementation when using `--backend-layout v1`.
- FUSE writeback cache is requested by default; when the kernel honors it, writes may be buffered/merged in the page cache, so durability still relies on `fsync`/`fdatasync`/close.
- Directory listings reconstruct original names and cache entries per-directory for the configured TTL (default 1s) and invalidate on mutations to cut repeated backend I/O. In v2 the same TTL controls an LRU of directory FDs to reduce open/close churn when resolving deep paths.
- Operations on `/` interact directly with the backend directory (chmod/chown/utimens supported; truncate disallowed).
- Extended attributes (get/set/list/remove) are forwarded to backend objects; `position` must be zero on Linux.
- `readdirplus` returns names with attributes; `flush`/`fsyncdir` are implemented; `poll` is accepted (returns no ready events).
- With `--backend-layout v1`, long names are stored in `<hash>n` namefiles and remain compatible with the C implementation. With `--backend-layout v2`, long names are mapped to internal `.__ln2_*` entries with the original bytes stored in `user.ln2.rawname`; internal names and `.ln2_index` are hidden from listings, and long-name hardlinks are rejected.

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
