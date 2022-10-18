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
use mvfs::{types::LockKind, vfs::AbstractHttpClient, Connection};
use slab::Slab;
use structopt::StructOpt;
use tokio::sync::Mutex;
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

const SYMLINK_INODE_BASE: u64 = 1048576 * 1;
const DB_INODE_BASE: u64 = 1048576 * 2;
const TEMP_INODE_BASE: u64 = 1048576 * 3;
const FAKE_INODE_BASE: u64 = 1048576 * 4;

lazy_static::lazy_static! {
    static ref DB_NAME_REGEX: regex::Regex = regex::Regex::new(r"^ns(\d+)-(\d+)$").unwrap();
}

#[derive(Copy, Clone)]
struct DbName {
    ns: usize,
    fakeino_delta: u64,
}

fn parse_db_name(name: &str) -> Option<DbName> {
    let captures = DB_NAME_REGEX.captures(name)?;
    let ns = captures.get(1).unwrap().as_str().parse().ok()?;
    let fakeino_delta = captures.get(2).unwrap().as_str().parse().ok()?;
    Some(DbName { ns, fakeino_delta })
}

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
        http_client: AbstractHttpClient::Prebuilt(reqwest::Client::new()),
        db_name_map: Arc::new(Default::default()),
        lock_owner: None,
        fork_tolerant: false,
    };
    let fuse_fs = FuseFs {
        namespaces,
        vfs,
        ephemeral_inodes: Slab::new(),
        ns_to_ephemeral_inode: BTreeMap::new(),
        temp_inodes: Slab::new(),
        symlink_inodes: Slab::new(),
        next_fh: 1,
        next_fakeino_delta: 0,
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
    temp_inodes: Slab<TempInode>,
    symlink_inodes: Slab<SymlinkInode>,
    next_fh: u64,
    next_fakeino_delta: u64,
}

struct EphemeralInode {
    refcount: u64,
    ns: usize,
    conn: Option<Arc<Mutex<Connection>>>,
    locks: BTreeMap<(u64, u64), LockKind>,
    fakeino: u64,
}

struct TempInode {
    refcount: u64,
    data: Vec<u8>,
    fakeino: u64,
}

struct SymlinkInode {
    refcount: u64,
    target: Vec<u8>,
}

impl FuseFs {
    fn lock_bookkeeping(&mut self, ino: u64, reply: fuser::ReplyEmpty) {
        let rawino = (ino - DB_INODE_BASE) as usize;
        let inode = self.ephemeral_inodes.get(rawino).unwrap();
        let (ns_name, _) = self.namespaces.get_index(inode.ns).unwrap();
        let ns_name = ns_name.clone();
        let desired = inode
            .locks
            .values()
            .cloned()
            .max()
            .unwrap_or(LockKind::None);
        let conn = inode.conn.as_ref().unwrap().clone();
        tokio::spawn(async move {
            let mut conn = conn.lock().await;
            let current = conn.current_lock();
            tracing::debug!(ns = ns_name, ?current, ?desired, "lock_bookkeeping");
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

        if let Some(ns) = self.namespaces.get_index_of(name) {
            let fakeino_delta = self.next_fakeino_delta;
            self.next_fakeino_delta += 1;
            let rawino = self.symlink_inodes.insert(SymlinkInode {
                refcount: 1,
                target: Vec::from(format!("ns{}-{}.db", ns, fakeino_delta)),
            }) as u64;
            let ino = rawino + SYMLINK_INODE_BASE;
            let attr = FileAttr {
                ino,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH, // 1970-01-01 00:00:00
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::Symlink,
                perm: 0o777,
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

        if name.ends_with(".db") {
            let name_prefix = name.trim_end_matches(".db");
            let name =
                parse_db_name(name_prefix).filter(|x| self.namespaces.get_index(x.ns).is_some());

            if let Some(name) = name {
                let rawino = self.ephemeral_inodes.insert(EphemeralInode {
                    refcount: 1,
                    ns: name.ns,
                    conn: None,
                    locks: BTreeMap::new(),
                    fakeino: (FAKE_INODE_BASE + name.fakeino_delta) * 2,
                }) as u64;
                let ino = rawino + DB_INODE_BASE;
                assert!(ino < TEMP_INODE_BASE);
                self.ns_to_ephemeral_inode
                    .entry(name.ns)
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
        } else if name.ends_with(".db-journal") {
            let name_prefix = name.trim_end_matches(".db-journal");
            let name =
                parse_db_name(name_prefix).filter(|x| self.namespaces.get_index(x.ns).is_some());

            if let Some(name) = name {
                let rawino = self.temp_inodes.insert(TempInode {
                    refcount: 1,
                    data: vec![],
                    fakeino: (FAKE_INODE_BASE + name.fakeino_delta) * 2 + 1,
                }) as u64;
                let ino = rawino + TEMP_INODE_BASE;
                let attr = FileAttr {
                    ino,
                    size: 0,
                    blocks: 0,
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
        if ino >= SYMLINK_INODE_BASE && ino < DB_INODE_BASE {
            let rawino = (ino - SYMLINK_INODE_BASE) as usize;
            let inode = self.symlink_inodes.get_mut(rawino).unwrap();
            assert!(inode.refcount >= nlookup);
            inode.refcount -= nlookup;
            if inode.refcount == 0 {
                self.symlink_inodes.remove(rawino);
                tracing::debug!(ino, "removed symlink inode");
            }
        } else if ino >= DB_INODE_BASE && ino < TEMP_INODE_BASE {
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
        } else if ino >= TEMP_INODE_BASE {
            let rawino = (ino - TEMP_INODE_BASE) as usize;
            let inode = self.temp_inodes.get_mut(rawino).unwrap();
            assert!(inode.refcount >= nlookup);
            inode.refcount -= nlookup;
            if inode.refcount == 0 {
                self.temp_inodes.remove(rawino);
                tracing::debug!(ino, "removed temp inode");
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
            let conn = match self.vfs.open(namespace, false) {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::error!(ns = ns_name, error = %e, "failed to open namespace");
                    reply.error(libc::EIO);
                    return;
                }
            };
            inode.conn = Some(Arc::new(Mutex::new(conn)));
            let fh = self.next_fh;
            self.next_fh += 1;
            reply.opened(fh, FOPEN_DIRECT_IO);
            return;
        } else if ino >= TEMP_INODE_BASE {
            let fh = self.next_fh;
            self.next_fh += 1;
            reply.opened(fh, FOPEN_DIRECT_IO);
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

        let rawino = (ino - DB_INODE_BASE) as usize;
        let inode = self.ephemeral_inodes.get_mut(rawino).unwrap();
        // Delete overlapping locks
        let keys_to_delete = inode
            .locks
            .keys()
            .filter(|k| k.0 >= start && k.1 <= end)
            .copied()
            .collect::<Vec<_>>();
        for k in keys_to_delete {
            inode.locks.remove(&k);
        }
        let kind = lock_type_to_lockkind(typ);
        if kind != LockKind::None {
            inode.locks.insert((start, end), kind);
        }
        self.lock_bookkeeping(ino, reply);
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
        if ino >= TEMP_INODE_BASE {
            let rawino = (ino - TEMP_INODE_BASE) as usize;
            let inode = self.temp_inodes.get_mut(rawino).unwrap();

            assert!(offset >= 0);

            let end = (offset as u64).saturating_add(size as u64);
            let saturated_offset = (offset as u64).min(inode.data.len() as u64);
            let saturated_end = end.min(inode.data.len() as u64);
            reply.data(&inode.data[saturated_offset as usize..saturated_end as usize]);
            return;
        }

        if !(ino >= DB_INODE_BASE && ino < TEMP_INODE_BASE) {
            tracing::debug!(ino, "unknown read");
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
        if ino >= TEMP_INODE_BASE {
            let rawino = (ino - TEMP_INODE_BASE) as usize;
            let inode = self.temp_inodes.get_mut(rawino).unwrap();

            assert!(offset >= 0);
            let end = (offset as u64).checked_add(data.len() as u64).unwrap();
            if end > inode.data.len() as u64 {
                inode.data.resize(end as usize, 0);
            }

            inode.data[offset as usize..end as usize].copy_from_slice(data);
            reply.written(data.len() as u32);
            return;
        }

        if !(ino >= DB_INODE_BASE && ino < TEMP_INODE_BASE) {
            tracing::debug!(ino, "unknown write");
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
        // We return a fake ino from this function - SQLite expects identity.
        let uid = req.uid();
        let gid = req.gid();
        let sector_size = self.vfs.sector_size;
        let mut attr = FileAttr {
            ino: 0,
            size: 0,
            blocks: 0,
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

        if ino >= TEMP_INODE_BASE {
            let rawino = (ino - TEMP_INODE_BASE) as usize;
            let inode = self.temp_inodes.get(rawino).unwrap();
            attr.ino = inode.fakeino;
            attr.size = inode.data.len() as u64;
            attr.blocks = inode.data.len() as u64 / sector_size as u64;
            reply.attr(&Duration::ZERO, &attr);
            return;
        }

        if ino >= SYMLINK_INODE_BASE && ino < DB_INODE_BASE {
            let rawino = (ino - SYMLINK_INODE_BASE) as usize;
            let inode = self.symlink_inodes.get(rawino).unwrap();
            attr.ino = ino;
            attr.size = inode.target.len() as u64;
            attr.blocks = 1;
            attr.kind = FileType::Symlink;
            attr.perm = 0o777;
            reply.attr(&Duration::ZERO, &attr);
            return;
        }

        if !(ino >= DB_INODE_BASE && ino < TEMP_INODE_BASE) {
            reply.error(libc::ENOENT);
            return;
        }

        let rawino = (ino - DB_INODE_BASE) as usize;
        let inode = self.ephemeral_inodes.get(rawino).unwrap();
        let ns = inode.ns;
        let fakeino = inode.fakeino;
        let (ns_name, _) = self.namespaces.get_index(ns).unwrap();
        let ns_name = ns_name.clone();
        tracing::debug!(ns = ns_name, ino, "getattr");
        if let Some(conn) = &inode.conn {
            let conn = conn.clone();
            tokio::spawn(async move {
                let mut conn = conn.lock().await;
                match conn.size().await {
                    Ok(x) => {
                        attr.ino = fakeino;
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
            attr.ino = fakeino;
            attr.size = sector_size as u64;
            attr.blocks = 1;
            reply.attr(&Duration::ZERO, &attr);
        }
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        _parent: u64,
        _name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn readlink(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        if ino >= SYMLINK_INODE_BASE && ino < DB_INODE_BASE {
            let rawino = (ino - SYMLINK_INODE_BASE) as usize;
            let inode = self.symlink_inodes.get(rawino).unwrap();
            let mut data: Vec<u8> = Vec::with_capacity(inode.target.len() + 1);
            data.extend_from_slice(&inode.target);
            data.push(0);
            reply.data(&data);
            return;
        }

        reply.error(libc::ENOENT);
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
