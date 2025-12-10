mod config;
mod fs;
mod handle_table;
mod namefile;
mod pathmap;
mod util;
mod v2;

use crate::v2::{IndexSync, LongNameFsV2Fuser};
use clap::{Parser, ValueEnum};
use config::Config;
use fs::LongNameFs;
use fuse3::MountOptions;
use fuse3::path::PathFilesystem;
use fuse3::path::Session;
use fuser::{MountOption as FuserMountOption, Session as FuserSession};
#[cfg(unix)]
use futures_util::future::poll_fn;
#[cfg(unix)]
use nix::mount::{MntFlags, umount2};
#[cfg(unix)]
use std::cmp::min;
use std::path::PathBuf;
#[cfg(unix)]
use std::pin::Pin;
use std::time::Duration;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
#[cfg(unix)]
use tokio::sync::oneshot;

#[cfg(unix)]
fn increase_rlimit_nofile() -> std::io::Result<()> {
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            let desired = min(65536 as libc::rlim_t, limit.rlim_max);
            if desired > limit.rlim_cur {
                let new_limit = libc::rlimit {
                    rlim_cur: desired,
                    rlim_max: limit.rlim_max,
                };
                let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &new_limit);
            }
        }
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "longnamefs-rs")]
#[command(about = "FUSE3 long file name shim compatible with the C longnamefs backend layout")]
struct Cli {
    /// Backend layout version (v1: current hash+namefile; v2: xattr+index, WIP).
    #[arg(long, value_enum, default_value_t = BackendLayout::V1)]
    backend_layout: BackendLayout,

    /// Backend directory where hashed names and namefiles are stored.
    #[arg(long)]
    backend: PathBuf,

    /// Mount point for the virtual filesystem.
    mountpoint: PathBuf,

    /// Allow other users to access the mount (passes allow_other to FUSE).
    #[arg(long, default_value_t = false)]
    allow_other: bool,

    /// Select FUSE binding (v1 uses fuse3; v2 always uses fuser; this flag is ignored for v2).
    #[arg(long, value_enum, default_value_t = FuseImpl::Fuse3)]
    fuse_impl: FuseImpl,

    /// Permit mounting on a non-empty directory.
    #[arg(long, default_value_t = false)]
    nonempty: bool,

    /// Directory cache TTL in milliseconds. Set to 0 to disable, or use --no-dir-cache.
    #[arg(long, default_value_t = 1000)]
    dir_cache_ttl_ms: u64,

    /// Disable directory entry cache.
    #[arg(long, default_value_t = false)]
    no_dir_cache: bool,

    /// fdatasync data files on write to improve durability (at the cost of latency).
    #[arg(long, default_value_t = false)]
    sync_data: bool,

    /// Check and avoid hash collisions by probing suffixed entries; slightly slower.
    #[arg(long, default_value_t = false)]
    collision_protect: bool,

    /// Maximum write size advertised to FUSE in KiB (default 1024; kernel may clamp to its own maximum).
    #[arg(long, default_value_t = 1024)]
    max_write_kb: u32,

    /// Maximum logical path segment length allowed (v2 only; enforced before hashing).
    #[arg(long, default_value_t = 1024)]
    max_name_len: usize,

    /// TTL for attr/entry replies in milliseconds (v2 only). Set to 0 to disable kernel caching.
    #[arg(long)]
    attr_ttl_ms: Option<u64>,

    /// v2 index flush strategy: always sync, batch by time/ops, or off (no flush).
    #[arg(long, value_enum, default_value_t = IndexSyncCli::Batch)]
    index_sync: IndexSyncCli,

    /// Use non-transactional namefile writes (faster but unsafe: filename metadata may be lost or become inconsistent on crash).
    #[arg(long, default_value_t = false)]
    unsafe_namefile_writes: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum BackendLayout {
    V1,
    V2,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum FuseImpl {
    Fuse3,
    Fuser,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum IndexSyncCli {
    Always,
    Batch,
    Off,
}

impl From<IndexSyncCli> for IndexSync {
    fn from(value: IndexSyncCli) -> Self {
        match value {
            IndexSyncCli::Always => IndexSync::Always,
            IndexSyncCli::Batch => IndexSync::Batch {
                max_pending: 128,
                max_age: Duration::from_secs(5),
            },
            IndexSyncCli::Off => IndexSync::Off,
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    #[cfg(unix)]
    let _ = increase_rlimit_nofile();

    let cli = Cli::parse();
    let mountpoint = cli.mountpoint.clone();

    let config = Config::open_backend(cli.backend, cli.sync_data, cli.collision_protect)
        .map_err(std::io::Error::from)
        .map_err(anyhow::Error::from)?;

    let cache_ttl = if cli.no_dir_cache || cli.dir_cache_ttl_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(cli.dir_cache_ttl_ms))
    };
    let attr_ttl = cli
        .attr_ttl_ms
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_secs(1));

    let mut mount_opts = MountOptions::default();
    mount_opts.fs_name("longnamefs-rs");
    mount_opts.allow_other(cli.allow_other);
    mount_opts.nonempty(cli.nonempty);
    mount_opts.write_back(true);

    match cli.backend_layout {
        BackendLayout::V1 => {
            namefile::set_relaxed_namefile_mode(cli.unsafe_namefile_writes);
            if cli.fuse_impl != FuseImpl::Fuse3 {
                anyhow::bail!("fuser adapter is not implemented for backend layout v1");
            }
            let fs = LongNameFs::new(config, cache_ttl, cli.max_write_kb);
            run_mount(fs, mountpoint, mount_opts).await
        }
        BackendLayout::V2 => {
            eprintln!(
                "WARNING: backend layout v2 is experimental and incompatible with v1 data; use a dedicated empty backend directory."
            );
            if cli.fuse_impl == FuseImpl::Fuse3 {
                eprintln!("v2 now only supports the fuser adapter; falling back to fuser.");
            }
            let fs = LongNameFsV2Fuser::new(
                config,
                cli.max_name_len,
                cache_ttl,
                cli.max_write_kb,
                cli.index_sync.into(),
                attr_ttl,
            )
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            run_mount_fuser(fs, mountpoint, cli.allow_other, cli.nonempty)
        }
    }
}

async fn run_mount<F>(fs: F, mountpoint: PathBuf, mount_opts: MountOptions) -> anyhow::Result<()>
where
    F: PathFilesystem + Send + Sync + 'static,
{
    let session = Session::new(mount_opts);
    let handle = session.mount(fs, mountpoint.clone()).await?;

    #[cfg(unix)]
    {
        async fn detach_mountpoint(path: &std::path::Path) -> anyhow::Result<()> {
            let owned = path.to_owned();
            tokio::task::spawn_blocking(move || {
                umount2(&owned, MntFlags::MNT_DETACH).map_err(anyhow::Error::from)
            })
            .await?
        }

        fn is_ebusy(err: &anyhow::Error) -> bool {
            err.downcast_ref::<std::io::Error>()
                .and_then(|io| io.raw_os_error())
                == Some(libc::EBUSY)
        }

        // Listen for termination signals and unmount cleanly before exiting.
        let (unmount_tx, unmount_rx) = oneshot::channel::<()>();

        let mut mount_task = tokio::spawn(async move {
            let mut handle = Some(handle);
            let mut handle_future = poll_fn(|cx| {
                let handle = handle.as_mut().expect("mount handle missing");
                Pin::new(handle).poll(cx)
            });

            let res = tokio::select! {
                res = &mut handle_future => res,
                _ = unmount_rx => {
                    let handle = handle.take().expect("mount handle missing");
                    handle.unmount().await
                }
            };

            res.map_err(anyhow::Error::from)
        });

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;

        let signals = async {
            tokio::select! {
                _ = sigint.recv() => (),
                _ = sigterm.recv() => (),
            }
        };
        tokio::pin!(signals);

        let result = tokio::select! {
            res = &mut mount_task => res,
            _ = &mut signals => {
                let _ = unmount_tx.send(());
                mount_task.await
            }
        };

        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) if is_ebusy(&err) => {
                detach_mountpoint(&mountpoint).await?;
            }
            Ok(Err(err)) => return Err(err),
            Err(join_err) => return Err(join_err.into()),
        }
    }

    #[cfg(not(unix))]
    {
        // Block until the filesystem is unmounted. This keeps the
        // process alive instead of exiting immediately after mount.
        handle.await?;
    }

    Ok(())
}

fn run_mount_fuser(
    fs: LongNameFsV2Fuser,
    mountpoint: PathBuf,
    allow_other: bool,
    nonempty: bool,
) -> anyhow::Result<()> {
    let mut options = vec![
        FuserMountOption::RW,
        FuserMountOption::FSName("longnamefs-rs".to_string()),
        FuserMountOption::Subtype("ln2".to_string()),
    ];
    if allow_other {
        options.push(FuserMountOption::AllowOther);
    }
    if nonempty {
        options.push(FuserMountOption::CUSTOM("nonempty".to_string()));
    }
    let notifier = fs.notifier_handle();
    let mut session = FuserSession::new(fs, mountpoint, &options).map_err(anyhow::Error::from)?;
    notifier.set(session.notifier());
    session.run().map_err(anyhow::Error::from)
}
