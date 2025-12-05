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
use std::path::PathBuf;
use std::time::Duration;

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
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = Config::open_backend(cli.backend)
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

    // Block until the filesystem is unmounted. This keeps the
    // process alive instead of exiting immediately after mount.
    handle.await?;

    Ok(())
}
