longnamefs-rs
=============

Rust FUSE implementation (v1 via fuse3 async, v2 via fuser sync) of a “long filename shim” with two backend layouts:

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
  [--backend-layout v1|v2] \
  [--allow-other] [--nonempty] \
  [--dir-cache-ttl-ms 1000 | --no-dir-cache] \
  [--max-write-kb 1024] [--sync-data] \
  [layout-specific options...]
```

**Common options (v1 and v2)**
- `--backend` (required): backend directory on disk.
- `--backend-layout` (`v1` default): choose layout; only `v1` is compatible with the C implementation, `v2` requires its own backend.
- `--fuse-impl` (`fuse3` default): binding selection for v1. `fuser` is currently **not implemented** for v1 (the program will exit with an error). For v2 this flag is ignored and `fuser` is always used.
- `--allow-other` (default off): pass `allow_other` to FUSE.
- `--nonempty` (default off): allow mounting on a non-empty mountpoint.
- `--dir-cache-ttl-ms`: per-directory readdir cache TTL in milliseconds (default 1000); also drives the directory FD cache used by the v2 path resolver.
- `--no-dir-cache` (default off): disable directory cache (useful for debugging).
- `--max-write-kb`: maximum write size advertised to FUSE in KiB (default 1024; kernel may clamp).
- `--sync-data` (default off): fdatasync after writes for stronger durability (at the cost of throughput).
- The process attempts to raise `RLIMIT_NOFILE` on Unix to reduce `EMFILE` risk when many directory FDs are cached; set a generous `ulimit -n` (e.g. 65536) in production.

**v1 (hash + namefile, C-compatible)**
- `--collision-protect` (default off): probe namefiles to detect/avoid hash collisions by using suffixed `<hash>.k` entries (adds I/O).
- `--unsafe-namefile-writes` (default off): use non-transactional namefile updates (faster for many small files, but filename metadata may be lost or become inconsistent on crash).

**v2 (xattr + index, incompatible with C/v1 backends)**
- Uses the fuser adapter exclusively; the `--fuse-impl` switch is ignored for v2.
- `--max-name-len`: maximum logical segment length accepted (default 1024; returns `ENAMETOOLONG`/`EINVAL` when exceeded).
- `--index-sync` (`batch` default): `always` flush index on every mutation, `batch` flushes when 128 pending changes or 5s elapsed, `off` disables background flush (index rebuild will recover).
- `--attr-ttl-ms`: TTL for v2 attr/entry replies in milliseconds (default 1000). Set to 0 to disable kernel caching.
- `--open-ttl-ms`: TTL for v2 attr/entry replies of *open regular files* in milliseconds. Defaults to `--attr-ttl-ms` when omitted.
- `--enable-passthrough` (default off): request FUSE passthrough (kernel 7.40+ with `FUSE_PASSTHROUGH` required; fuser `abi-7-40` feature is compiled in by default). On failure to negotiate, IO automatically falls back to userspace and logs the reason once per process start.
- `--enable-writeback-cache` (default off): request FUSE writeback cache. When accepted, the kernel may buffer/merge writes; durability still requires fsync/fdatasync.
- `--enable-passthrough-meta-fd` (default off): enable caching/promoting a per-handle metadata FD when passthrough is active (reduces metadata overhead at the cost of extra FDs).
- `--passthrough-meta-fd-max` (default 1024): maximum cached meta FDs to keep.
- `--passthrough-meta-fd-min-open-count` (default 4): minimum handle open_count before considering meta-fd promotion.
- `--passthrough-meta-fd-min-lifetime-ms` (default 1000): minimum handle lifetime before considering promotion.
- `--passthrough-meta-fd-min-meta-ops` (default 8): minimum metadata operations before considering promotion.
- `--passthrough-meta-fd-cooldown-ms` (default 5000): cooldown after `EMFILE/ENFILE` while promoting meta FDs.

The process stays in the foreground and runs until the filesystem is unmounted
or the process is terminated. This is intentional and makes it easy to use
with process supervisors (e.g. systemd).

Behavior
--------

- Read/write, create, rename, link, symlink, mkdir/mknod, truncate, chmod/chown, utimens, and statfs mirror the C implementation when using `--backend-layout v1`.
- With `--backend-layout v1`, writeback caching is requested unconditionally (there is currently no CLI toggle). When the kernel honors it, writes may be buffered/merged in the page cache, so durability relies on explicit `fsync`/`fdatasync` (close does not force a sync) and the kernel's asynchronous flushes.
- With `--backend-layout v2`, writeback caching is **opt-in** via `--enable-writeback-cache`.
- Directory listings reconstruct original names and cache entries per-directory for the configured TTL (default 1s) and invalidate on mutations to cut repeated backend I/O. In v2 the same TTL controls an LRU of directory FDs to reduce open/close churn when resolving deep paths.
- Operations on `/` interact directly with the backend directory (chmod/chown/utimens supported; truncate disallowed).
- Extended attributes (get/set/list/remove) are forwarded to backend objects; `position` must be zero on Linux.
- `readdirplus` returns names with attributes; `flush`/`fsyncdir` are implemented; `poll` is accepted (returns no ready events).
- With `--backend-layout v1`, long names are stored in `<hash>n` namefiles and remain compatible with the C implementation. With `--backend-layout v2`, long names are mapped to internal `.__ln2_*` entries with the original bytes stored in `user.ln2.rawname`; FS-internal `.ln2_fs_*` entries (index/journal/tmp/probes) are hidden from listings, and long-name hardlinks are rejected.
- On SIGINT/SIGTERM the process attempts a lazy unmount (`MNT_DETACH`) and then exits immediately (it does not try to drain in-flight IO).

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
