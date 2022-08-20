use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use fuser::{
    consts::{FOPEN_DIRECT_IO, FUSE_POSIX_LOCKS},
    FileAttr, FileType, MountOption,
};
use indexmap::IndexMap;

use anyhow::Result;
use backtrace::Backtrace;
use mvfs::{types::LockKind, Connection};
use structopt::StructOpt;
use tokio::sync::Mutex;
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

const DB_INODE_BASE: u64 = 256;
const TEMP_INODE_BASE: u64 = 0xffffffff00000000;

#[derive(Debug, StructOpt)]
#[structopt(name = "mvsqlite-fuse", about = "mvsqlite fuse")]
struct Opt {
    /// Data plane URL.
    #[structopt(long, env = "MVSQLITE_FUSE_DATA_PLANE")]
    data_plane: String,

    /// Comma-separated namespace mappings: file1=ns1,file2=ns2
    #[structopt(long, env = "MVSQLITE_FUSE_NAMESPACES")]
    namespaces: String,

    /// Max concurrency.
    #[structopt(long, default_value = "10", env = "MVSQLITE_FUSE_CONCURRENCY")]
    concurrency: u32,

    /// Disk sector size.
    #[structopt(long, default_value = "8192", env = "MVSQLITE_FUSE_SECTOR_SIZE")]
    sector_size: usize,

    /// Mount point.
    #[structopt(long, env = "MVSQLITE_FUSE_MOUNTPOINT")]
    mountpoint: String,

    /// Auto unmount on process exit.
    #[structopt(long)]
    auto_unmount: bool,

    /// Allow root.
    #[structopt(long)]
    allow_root: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    SubscriberBuilder::default()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    std::panic::set_hook(Box::new(|info| {
        let bt = Backtrace::new();
        tracing::error!(backtrace = ?bt, info = %info, "panic");
        std::process::abort();
    }));

    let opt: Opt = Opt::from_args();
    let namespaces = opt
        .namespaces
        .split(',')
        .map(|s| {
            s.split_once('=')
                .unwrap_or_else(|| panic!("invalid namespace mapping: {}", s))
        })
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<IndexMap<_, _>>();
    let vfs = mvfs::MultiVersionVfs {
        data_plane: opt.data_plane.clone(),
        sector_size: opt.sector_size,
        http_client: reqwest::Client::new(),
    };
    let fuse_fs = FuseFs {
        namespaces,
        vfs,
        fh_table: BTreeMap::new(),
        next_fh: 1,
        next_ephemeral_inode: DB_INODE_BASE,
        ns_to_ephemeral_inode: BTreeMap::new(),
        ephemeral_inode_to_ns: BTreeMap::new(),
        inode2fh: BTreeMap::new(),
        concurrency: opt.concurrency,
    };

    let mut options = vec![MountOption::RO, MountOption::FSName("mvsqlite".to_string())];
    if opt.auto_unmount {
        options.push(MountOption::AutoUnmount);
    }
    if opt.allow_root {
        options.push(MountOption::AllowRoot);
    }

    tokio::task::block_in_place(|| fuser::mount2(fuse_fs, opt.mountpoint.clone(), &options))?;

    anyhow::bail!("mount2 returned");
}

struct FuseFs {
    namespaces: IndexMap<String, String>,
    vfs: mvfs::MultiVersionVfs,
    fh_table: BTreeMap<u64, FhEntry>,
    next_fh: u64,
    next_ephemeral_inode: u64,
    ns_to_ephemeral_inode: BTreeMap<u64, BTreeSet<u64>>,
    ephemeral_inode_to_ns: BTreeMap<u64, u64>,
    inode2fh: BTreeMap<u64, u64>,
    concurrency: u32,
}

struct FhEntry {
    conn: Arc<Mutex<Connection>>,
}

impl fuser::Filesystem for FuseFs {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        config
            .set_max_readahead(self.vfs.sector_size as u32)
            .unwrap();
        config.set_max_write(self.vfs.sector_size as u32).unwrap();
        config
            .add_capabilities(FUSE_POSIX_LOCKS)
            .unwrap();
        Ok(())
    }

    fn lookup(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let name = name.to_str().unwrap_or_default();
        if parent != 1 {
            reply.error(libc::ENOENT);
            return;
        }

        if name.ends_with(".db") {
            let name_prefix = name.trim_end_matches(".db");
            if let Some(index) = self.namespaces.get_index_of(name_prefix) {
                let ino = self.next_ephemeral_inode;
                assert!(ino < TEMP_INODE_BASE);
                self.next_ephemeral_inode += 1;
                self.ns_to_ephemeral_inode
                    .entry(index as u64)
                    .or_default()
                    .insert(ino);
                self.ephemeral_inode_to_ns.insert(ino, index as u64);

                let attr = FileAttr {
                    ino,
                    size: self.vfs.sector_size as u64,
                    blocks: 1,
                    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    crtime: UNIX_EPOCH,
                    kind: FileType::RegularFile,
                    perm: 0o644,
                    nlink: 1,
                    uid: req.uid(),
                    gid: req.gid(),
                    rdev: 0,
                    flags: 0,
                    blksize: self.vfs.sector_size as u32,
                };
                reply.entry(&Duration::ZERO, &attr, 0);
                return;
            }
        }

        reply.error(libc::ENOENT);
    }

    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        if let Some(&index) = self.ephemeral_inode_to_ns.get(&ino) {
            self.ns_to_ephemeral_inode
                .get_mut(&index)
                .unwrap()
                .remove(&ino);
            let index = index as usize;
            let (_, namespace) = self.namespaces.get_index(index).unwrap();
            let conn = match self.vfs.open(namespace) {
                Ok(conn) => conn,
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            };
            let fh = self.next_fh;
            self.fh_table.insert(
                fh,
                FhEntry {
                    conn: Arc::new(Mutex::new(conn)),
                },
            );
            self.next_fh += 1;
            self.inode2fh.insert(ino, fh);
            reply.opened(fh, FOPEN_DIRECT_IO);
        }
    }
    fn setlk(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        _lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        _pid: u32,
        sleep: bool,
        reply: fuser::ReplyEmpty,
    ) {
        assert!(!sleep, "blocking locks are not supported");

        if start == end {
            reply.ok();
            return;
        }

        let index = *self.ephemeral_inode_to_ns.get(&ino).unwrap();

        let conn = self.fh_table.get(&fh).unwrap().conn.clone();
        let (ns_name, _) = self.namespaces.get_index(index as usize).unwrap();
        let ns_name = ns_name.clone();
        let desired = lock_type_to_lockkind(typ);
        tokio::spawn(async move {
            let mut conn = conn.lock().await;
            let current = conn.current_lock();
            tracing::debug!(ns = ns_name, ?current, ?desired, start, end, "setlk");
            let result = if desired.level() > current.level() {
                conn.lock(desired).await
            } else if desired.level() < current.level() {
                conn.unlock(desired).await
            } else {
                Ok(true)
            };
            match result {
                Ok(true) => {
                    reply.ok();
                }
                Ok(false) => {
                    reply.error(libc::EWOULDBLOCK);
                }
                Err(e) => {
                    tracing::error!(ns = ns_name, error = %e, ?current, ?desired, "lock error");
                    reply.error(libc::EIO);
                }
            }
        });
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let removed = self.fh_table.remove(&fh);
        assert!(removed.is_some());

        let removed = self.inode2fh.remove(&ino);
        assert!(removed.is_some());

        let removed = self.ephemeral_inode_to_ns.remove(&ino);
        assert!(removed.is_some());

        reply.ok();
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let sector_size = self.vfs.sector_size;
        let index = *self.ephemeral_inode_to_ns.get(&ino).unwrap();
        let conn = self.fh_table.get(&fh).unwrap().conn.clone();
        let (ns_name, _) = self.namespaces.get_index(index as usize).unwrap();
        let ns_name = ns_name.clone();
        tracing::debug!(ns = ns_name, offset, size, "read");
        tokio::spawn(async move {
            let mut conn = conn.lock().await;
            let mut buf = vec![0u8; sector_size];
            match conn
                .read_exact_at(
                    &mut buf,
                    (offset as u64 / sector_size as u64) * sector_size as u64,
                )
                .await
            {
                Ok(()) => {
                    reply.data(fixup_read_shift(offset, size, &buf));
                }
                Err(e) => {
                    tracing::error!(ns = ns_name, error = %e, "read error");
                    reply.error(libc::EIO);
                }
            }
        });
    }

    fn flush(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok()
    }

    fn getattr(&mut self, req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        let uid = req.uid();
        let gid = req.gid();
        let sector_size = self.vfs.sector_size;
        let mut attr = FileAttr {
            ino,
            size: sector_size as u64,
            blocks: 1,
            atime: UNIX_EPOCH, // 1970-01-01 00:00:00
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            flags: 0,
            blksize: sector_size as u32,
        };
        if let Some(fh) = self.inode2fh.get(&ino) {
            let fh = *fh;
            let index = *self.ephemeral_inode_to_ns.get(&ino).unwrap();
            let (ns_name, _) = self.namespaces.get_index(index as usize).unwrap();
            let ns_name = ns_name.clone();
            tracing::debug!(ns = ns_name, fh, "getattr");
            let conn = self.fh_table.get(&fh).unwrap().conn.clone();
            tokio::spawn(async move {
                let mut conn = conn.lock().await;
                match conn.size().await {
                    Ok(x) => {
                        attr.size = x;
                        attr.blocks = x / sector_size as u64;
                        reply.attr(&Duration::ZERO, &attr);
                    }
                    Err(e) => {
                        tracing::error!(ns = ns_name, error = %e, "getattr error");
                        reply.error(libc::EIO);
                    }
                }
            });
        } else {
            reply.attr(&Duration::ZERO, &attr);
        }
    }
}

fn lock_type_to_lockkind(typ: i32) -> LockKind {
    match typ {
        libc::F_RDLCK => LockKind::Shared,
        libc::F_WRLCK => LockKind::Exclusive,
        libc::F_UNLCK => LockKind::None,
        _ => panic!("invalid lock type: {}", typ),
    }
}

fn fixup_read_shift(offset: i64, size: u32, buf: &[u8]) -> &[u8] {
    assert!(offset >= 0);
    let offset = offset as usize;
    let size = size as usize;

    assert!(size > 0);
    assert!(size <= buf.len());

    // Same page
    assert!(offset / buf.len() == (offset + size - 1) / buf.len());

    tracing::debug!(
        start = offset % buf.len(),
        end = offset % buf.len() + size,
        "read fixup"
    );
    &buf[offset % buf.len()..offset % buf.len() + size]
}
