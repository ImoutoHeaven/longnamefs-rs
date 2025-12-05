mod config;
mod fs;
mod handle_table;
mod namefile;
mod pathmap;
mod util;

use clap::Parser;
use config::Config;
use fs::LongNameFs;
use fuse3::MountOptions;
use fuse3::path::Session;
#[cfg(unix)]
use futures_util::future::poll_fn;
use std::path::PathBuf;
#[cfg(unix)]
use std::pin::Pin;
use std::time::Duration;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
#[cfg(unix)]
use tokio::sync::oneshot;

#[derive(Parser, Debug)]
#[command(name = "longnamefs-rs")]
#[command(about = "FUSE3 long file name shim compatible with the C longnamefs backend layout")]
struct Cli {
    /// Backend directory where hashed names and namefiles are stored.
    #[arg(long)]
    backend: PathBuf,

    /// Mount point for the virtual filesystem.
    mountpoint: PathBuf,

    /// Allow other users to access the mount (passes allow_other to FUSE).
    #[arg(long, default_value_t = false)]
    allow_other: bool,

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
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = Config::open_backend(cli.backend, cli.sync_data)
        .map_err(std::io::Error::from)
        .map_err(anyhow::Error::from)?;

    let cache_ttl = if cli.no_dir_cache || cli.dir_cache_ttl_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(cli.dir_cache_ttl_ms))
    };

    let fs = LongNameFs::new(config, cache_ttl);

    let mut mount_opts = MountOptions::default();
    mount_opts.fs_name("longnamefs-rs");
    mount_opts.allow_other(cli.allow_other);
    mount_opts.nonempty(cli.nonempty);

    let session = Session::new(mount_opts);
    let handle = session.mount(fs, cli.mountpoint).await?;

    #[cfg(unix)]
    {
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

        result??;
    }

    #[cfg(not(unix))]
    {
        // Block until the filesystem is unmounted. This keeps the
        // process alive instead of exiting immediately after mount.
        handle.await?;
    }

    Ok(())
}
