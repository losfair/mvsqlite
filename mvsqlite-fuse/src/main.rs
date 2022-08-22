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
use slab::Slab;
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
        ephemeral_inodes: Slab::new(),
        ns_to_ephemeral_inode: BTreeMap::new(),
    };

    let mut options = vec![
        MountOption::RW,
        MountOption::NoExec,
        MountOption::FSName("mvsqlite".to_string()),
    ];
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
    ephemeral_inodes: Slab<EphemeralInode>,
    ns_to_ephemeral_inode: BTreeMap<usize, BTreeSet<u64>>,
}

struct EphemeralInode {
    refcount: u64,
    ns: usize,
    conn: Option<Arc<Mutex<Connection>>>,
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
        config.add_capabilities(FUSE_POSIX_LOCKS).unwrap();
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
                let rawino = self.ephemeral_inodes.insert(EphemeralInode {
                    refcount: 1,
                    ns: index,
                    conn: None,
                }) as u64;
                let ino = rawino + DB_INODE_BASE;
                assert!(ino < TEMP_INODE_BASE);
                self.ns_to_ephemeral_inode
                    .entry(index)
                    .or_default()
                    .insert(ino);

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

    fn forget(&mut self, _req: &fuser::Request<'_>, ino: u64, nlookup: u64) {
        if ino >= DB_INODE_BASE && ino < TEMP_INODE_BASE {
            let rawino = (ino - DB_INODE_BASE) as usize;
            let inode = self.ephemeral_inodes.get_mut(rawino).unwrap();
            assert!(inode.refcount >= nlookup);
            inode.refcount -= nlookup;
            if inode.refcount == 0 {
                self.ns_to_ephemeral_inode
                    .get_mut(&inode.ns)
                    .unwrap()
                    .remove(&ino);
                self.ephemeral_inodes.remove(rawino);
                tracing::debug!(ino, "removed ephemeral inode");
            }
        }
    }

    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        if ino >= DB_INODE_BASE && ino < TEMP_INODE_BASE {
            let rawino = (ino - DB_INODE_BASE) as usize;
            let inode = self.ephemeral_inodes.get_mut(rawino).unwrap();
            assert!(inode.conn.is_none());
            self.ns_to_ephemeral_inode
                .get_mut(&inode.ns)
                .unwrap()
                .remove(&ino);
            let (ns_name, namespace) = self.namespaces.get_index(inode.ns).unwrap();
            let conn = match self.vfs.open(namespace) {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::error!(ns = ns_name, error = %e, "failed to open namespace");
                    reply.error(libc::EIO);
                    return;
                }
            };
            inode.conn = Some(Arc::new(Mutex::new(conn)));
            reply.opened(0, FOPEN_DIRECT_IO);
            return;
        }

        reply.error(libc::ENOENT);
    }

    fn setlk(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        _pid: u32,
        sleep: bool,
        reply: fuser::ReplyEmpty,
    ) {
        assert!(!sleep, "blocking locks are not supported");

        if !(ino >= DB_INODE_BASE && ino < TEMP_INODE_BASE) {
            reply.ok();
            return;
        }

        if start == end {
            reply.ok();
            return;
        }

        let rawino = (ino - DB_INODE_BASE) as usize;
        let inode = self.ephemeral_inodes.get(rawino).unwrap();
        let (ns_name, _) = self.namespaces.get_index(inode.ns).unwrap();
        let ns_name = ns_name.clone();
        let desired = lock_type_to_lockkind(typ);
        let conn = inode.conn.as_ref().unwrap().clone();
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
                    reply.error(libc::EACCES);
                }
                Err(e) => {
                    tracing::error!(ns = ns_name, error = %e, ?current, ?desired, "lock error");
                    reply.error(libc::EIO);
                }
            }
        });
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        if !(ino >= DB_INODE_BASE && ino < TEMP_INODE_BASE) {
            reply.error(libc::EIO);
            return;
        }
        let rawino = (ino - DB_INODE_BASE) as usize;
        let sector_size = self.vfs.sector_size;
        let inode = self.ephemeral_inodes.get(rawino).unwrap();
        let conn = inode.conn.as_ref().unwrap().clone();
        let (ns_name, _) = self.namespaces.get_index(inode.ns).unwrap();
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

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        if !(ino >= DB_INODE_BASE && ino < TEMP_INODE_BASE) {
            reply.error(libc::EIO);
            return;
        }
        let rawino = (ino - DB_INODE_BASE) as usize;
        let inode = self.ephemeral_inodes.get(rawino).unwrap();
        let conn = inode.conn.as_ref().unwrap().clone();
        let (ns_name, _) = self.namespaces.get_index(inode.ns).unwrap();
        let ns_name = ns_name.clone();
        tracing::debug!(ns = ns_name, offset, size = data.len(), "write");
        let data = data.to_vec();
        tokio::spawn(async move {
            let mut conn = conn.lock().await;
            match conn.write_all_at(&data, offset as u64).await {
                Ok(()) => {
                    reply.written(data.len() as _);
                }
                Err(e) => {
                    tracing::error!(ns = ns_name, error = %e, "write error");
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

        if !(ino >= DB_INODE_BASE && ino < TEMP_INODE_BASE) {
            reply.attr(&Duration::ZERO, &attr);
            return;
        }
        let rawino = (ino - DB_INODE_BASE) as usize;
        let inode = self.ephemeral_inodes.get(rawino).unwrap();
        let (ns_name, _) = self.namespaces.get_index(inode.ns).unwrap();
        let ns_name = ns_name.clone();
        tracing::debug!(ns = ns_name, ino, "getattr");
        if let Some(conn) = &inode.conn {
            let conn = conn.clone();
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
