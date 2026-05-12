/*
 * Copyright © 2018, Steve Smith <tarkasteve@gmail.com>
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License version
 * 3 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::collections::{HashMap, HashSet};
use std::os::unix::fs::{chown, MetadataExt, PermissionsExt};
use std::{cmp, thread};
use std::fs::{self, canonicalize, create_dir_all, read_link, File, Metadata};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crossbeam_channel as cbc;
use libfs::{
    allocate_file, copy_file_bytes, copy_owner, copy_permissions, copy_timestamps, next_sparse_segments, probably_sparse, reflink, sync, sync_xattrs, FileType
};
use log::{debug, error, info, warn};
use walkdir::WalkDir;

use crate::backup::{get_backup_path, needs_backup};
use crate::config::{Config, Reflink};
use crate::errors::{Result, XcpError};
use crate::feedback::{StatusUpdate, StatusUpdater};
use crate::paths::{parse_ignore, ignore_filter};

#[derive(Debug)]
pub struct CopyHandle {
    pub infd: File,
    pub outfd: File,
    pub metadata: Metadata,
    pub config: Arc<Config>,
}

impl CopyHandle {
    pub fn new(from: &Path, to: &Path, config: &Arc<Config>) -> Result<CopyHandle> {
        let infd = File::open(from)?;
        let metadata = infd.metadata()?;

        if needs_backup(to, config)? {
            let backup = get_backup_path(to)?;
            info!("Backup: Rename {to:?} to {backup:?}");
            fs::rename(to, backup)?;
        }

        let outfd = File::create(to)?;
        allocate_file(&outfd, metadata.len())?;

        let handle = CopyHandle {
            infd,
            outfd,
            metadata,
            config: config.clone(),
        };

        Ok(handle)
    }

    /// Copy len bytes from wherever the descriptor cursors are set.
    fn copy_bytes(&self, len: u64, updates: &Arc<dyn StatusUpdater>) -> Result<u64> {
        let mut written = 0;
        while written < len {
            let bytes_to_copy = cmp::min(len - written, self.config.block_size);
            let bytes = copy_file_bytes(&self.infd, &self.outfd, bytes_to_copy)? as u64;
            written += bytes;
            updates.send(StatusUpdate::Copied(bytes))?;
        }

        Ok(written)
    }

    /// Wrapper around copy_bytes that looks for sparse blocks and skips them.
    fn copy_sparse(&self, updates: &Arc<dyn StatusUpdater>) -> Result<u64> {
        let len = self.metadata.len();
        let mut pos = 0;

        while pos < len {
            let (next_data, next_hole) = next_sparse_segments(&self.infd, &self.outfd, pos)?;

            let _written = self.copy_bytes(next_hole - next_data, updates)?;
            pos = next_hole;
        }

        Ok(len)
    }

    pub fn try_reflink(&self) -> Result<bool> {
        match self.config.reflink {
            Reflink::Always | Reflink::Auto => {
                debug!("Attempting reflink from {:?}->{:?}", self.infd, self.outfd);
                let worked = reflink(&self.infd, &self.outfd)?;
                if worked {
                    debug!("Reflink {:?} succeeded", self.outfd);
                    Ok(true)
                } else if self.config.reflink == Reflink::Always {
                    Err(XcpError::ReflinkFailed(format!("{:?}->{:?}", self.infd, self.outfd)).into())
                } else {
                    debug!("Failed to reflink, falling back to copy");
                    Ok(false)
                }
            }

            Reflink::Never => {
                Ok(false)
            }
        }
    }

    pub fn copy_file(&self, updates: &Arc<dyn StatusUpdater>) -> Result<u64> {
        if self.try_reflink()? {
            return Ok(self.metadata.len());
        }
        let total = if probably_sparse(&self.infd)? {
            self.copy_sparse(updates)?
        } else {
            self.copy_bytes(self.metadata.len(), updates)?
        };

        Ok(total)
    }

    fn finalise_copy(&self) -> Result<()> {
        if !self.config.no_perms {
            copy_permissions(&self.infd, &self.outfd)?;
        }
        if !self.config.no_timestamps {
            copy_timestamps(&self.infd, &self.outfd)?;
        }
        if self.config.ownership && copy_owner(&self.infd, &self.outfd).is_err() {
            warn!("Failed to copy file ownership: {:?}", self.infd);
        }
        if self.config.fsync {
            debug!("Syncing file {:?}", self.outfd);
            sync(&self.outfd)?;
        }
        Ok(())
    }
}

impl Drop for CopyHandle {
    fn drop(&mut self) {
        // FIXME: Should we check for panicking() here?
        if let Err(e) = self.finalise_copy() {
            error!("Error during finalising copy operation {:?} -> {:?}: {}", self.infd, self.outfd, e);
        }
    }
}

#[derive(Debug)]
pub enum Operation {
    Copy(PathBuf, PathBuf),
    Link(PathBuf, PathBuf),
    Special(PathBuf, PathBuf),
}

pub fn tree_walker(
    sources: Vec<PathBuf>,
    dest: &Path,
    config: &Config,
    work_tx: cbc::Sender<Operation>,
    stats: Arc<dyn StatusUpdater>,
) -> Result<()> {
    debug!("Starting walk worker {:?}", thread::current().id());

    for source in sources {
        let sourcedir = source
            .components()
            .next_back()
            .ok_or(XcpError::InvalidSource("Failed to find source directory name."))?;

        let target_base = if dest.exists() && dest.is_dir() && !config.no_target_directory {
            dest.join(sourcedir)
        } else {
            dest.to_path_buf()
        };
        debug!("Target base is {target_base:?}");

        let gitignore = parse_ignore(&source, config)?;

        for entry in WalkDir::new(&source).follow_root_links(false)
            .into_iter()
            .filter_entry(|e| ignore_filter(e, &gitignore))
        {
            debug!("Got tree entry {entry:?}");
            let epath = entry?.into_path();
            let from = if config.dereference {
                let cpath = canonicalize(&epath)?;
                debug!("Dereferencing {epath:?} into {cpath:?}");
                cpath
            } else {
                epath.clone()
            };
            let meta = from.symlink_metadata()?;
            let path = epath.strip_prefix(&source)?;
            let target = if !empty_path(path) {
                target_base.join(path)
            } else {
                target_base.clone()
            };

            if config.no_clobber && target.exists() {
                let msg = "Destination file exists and --no-clobber is set.";
                stats.send(StatusUpdate::Error(
                    XcpError::DestinationExists(msg, target)))?;
                return Err(XcpError::EarlyShutdown(msg).into());
            }

            let ft = FileType::from(meta.file_type());
            match ft {
                FileType::File => {
                    debug!("Send copy operation {from:?} to {target:?}");
                    stats.send(StatusUpdate::Size(meta.len()))?;
                    work_tx.send(Operation::Copy(from, target))?;
                }

                FileType::Symlink => {
                    let lfile = read_link(from)?;
                    debug!("Send symlink operation {lfile:?} to {target:?}");
                    work_tx.send(Operation::Link(lfile, target))?;
                }

                FileType::Dir => {
                    // Create dir tree immediately as we can't
                    // guarantee a worker will action the creation
                    // before a subsequent copy operation requires it.
                    debug!("Creating target directory {target:?}");
                    if let Err(err) = create_dir_all(&target) {
                        let msg = format!("Error creating target directory: {err}");
                        error!("{msg}");
                        return Err(XcpError::CopyError(msg).into())
                    }
                    if config.ownership &&
                        let Err(e) = chown(&target, Some(meta.uid()), Some(meta.gid()))
                    {
                        warn!("Failed to copy directory ownership: {target:?}: {e}");
                    }
                }

                FileType::Socket | FileType::Char | FileType::Fifo => {
                    debug!("Special file found: {from:?} to {target:?}");
                    work_tx.send(Operation::Special(from, target))?;
                }

                FileType::Block => {
                    if config.copy_special {
                        debug!("Block device found: {from:?} to {target:?}");
                        work_tx.send(Operation::Special(from, target))?;
                    } else {
                        error!("Block device found but --special not set: {target:?}");
                        return Err(XcpError::UnknownFileType(target).into());
                    }
                }

                FileType::Other => {
                    error!("Unsupported filetype found: {target:?} -> {ft:?}");
                    return Err(XcpError::UnknownFileType(target).into());
                }
            };
        }
    }
    debug!("Walk-worker finished: {:?}", thread::current().id());

    Ok(())
}

fn empty_path(path: &Path) -> bool {
    *path == PathBuf::new()
}

/// Return true when a source file must be copied over an existing destination
/// file. Compares mtime (nanosecond precision), size, and optionally permissions.
fn needs_copy(src: &Metadata, dst: &Metadata, config: &Config) -> bool {
    if src.len() != dst.len() {
        return true;
    }
    if src.mtime() != dst.mtime() || src.mtime_nsec() != dst.mtime_nsec() {
        return true;
    }
    if !config.no_perms && src.permissions() != dst.permissions() {
        return true;
    }
    false
}

/// Walk `source`, emitting copy/link/special operations for entries that are
/// new or changed relative to `dest`, then delete any `dest` entries that are
/// no longer present in `source`. The combination makes `dest` identical to
/// `source` without using rsync-style block deltas.
pub fn sync_walker(
    source: &Path,
    dest: &Path,
    config: &Arc<Config>,
    work_tx: cbc::Sender<Operation>,
    stats: Arc<dyn StatusUpdater>,
) -> Result<()> {
    debug!("Starting sync walk worker {:?}", thread::current().id());

    if !dest.exists() {
        create_dir_all(dest)?;
    }

    let mut source_relpaths: HashSet<PathBuf> = HashSet::new();
    // Maps (dev, ino) of hard-linked source files to the first dest path written.
    let mut hardlink_map: HashMap<(u64, u64), PathBuf> = HashMap::new();
    let gitignore = parse_ignore(source, config)?;

    for entry in WalkDir::new(source)
        .follow_root_links(false)
        .into_iter()
        .filter_entry(|e| ignore_filter(e, &gitignore))
    {
        let epath = entry?.into_path();
        let from = if config.dereference {
            let cpath = canonicalize(&epath)?;
            debug!("Dereferencing {epath:?} into {cpath:?}");
            cpath
        } else {
            epath.clone()
        };

        let meta = from.symlink_metadata()?;
        let rel = epath.strip_prefix(source)?;
        source_relpaths.insert(rel.to_path_buf());

        let target = if !empty_path(rel) {
            dest.join(rel)
        } else {
            dest.to_path_buf()
        };

        let ft = FileType::from(meta.file_type());
        match ft {
            FileType::Dir => {
                match target.symlink_metadata() {
                    Ok(m) if !m.file_type().is_dir() => {
                        // Target exists but is a file or symlink: remove it
                        // so we can create a directory in its place.
                        debug!("Sync: removing non-directory blocking {target:?}");
                        fs::remove_file(&target)?;
                        create_dir_all(&target)?;
                    }
                    Err(_) => {
                        debug!("Sync: creating directory {target:?}");
                        if let Err(err) = create_dir_all(&target) {
                            let msg = format!("Error creating target directory: {err}");
                            error!("{msg}");
                            return Err(XcpError::CopyError(msg).into());
                        }
                    }
                    Ok(_) => {} // already a directory
                }
                if config.ownership {
                    if let Err(e) = chown(&target, Some(meta.uid()), Some(meta.gid())) {
                        warn!("Failed to copy directory ownership: {target:?}: {e}");
                    }
                }
                if !config.no_perms {
                    if let Ok(dst_meta) = target.symlink_metadata() {
                        if dst_meta.permissions() != meta.permissions() {
                            fs::set_permissions(&target, meta.permissions())?;
                        }
                    }
                }
                if config.copy_xattrs {
                    if let Err(e) = sync_xattrs(&from, &target) {
                        warn!("Failed to sync xattrs for directory {from:?}: {e}");
                    }
                }
            }

            FileType::File => {
                if config.preserve_hardlinks && meta.nlink() > 1 {
                    let key = (meta.dev(), meta.ino());
                    if let Some(first_dest) = hardlink_map.get(&key).cloned() {
                        // Subsequent occurrence of the same inode: ensure dest is
                        // a hard link to first_dest, handling any type conflicts.
                        let needs_relink = match target.symlink_metadata() {
                            Err(_) => true, // target absent
                            Ok(dst_meta) if !dst_meta.file_type().is_file() => true, // wrong type (dir or symlink)
                            Ok(dst_meta) => match first_dest.symlink_metadata() {
                                Ok(fd_meta) => {
                                    dst_meta.ino() != fd_meta.ino()
                                        || dst_meta.dev() != fd_meta.dev()
                                }
                                Err(_) => true, // first_dest gone
                            },
                        };
                        if needs_relink {
                            if let Ok(m) = target.symlink_metadata() {
                                if m.file_type().is_dir() {
                                    fs::remove_dir_all(&target)?;
                                } else {
                                    fs::remove_file(&target)?;
                                }
                            }
                            debug!("Sync: hard-link {first_dest:?} -> {target:?}");
                            fs::hard_link(&first_dest, &target)?;
                        } else {
                            debug!("Sync: skip unchanged hard-link {from:?}");
                        }
                    } else {
                        // First occurrence of this inode: copy synchronously so
                        // subsequent occurrences can hard-link to it immediately.
                        hardlink_map.insert(key, target.clone());
                        let should_copy = match target.symlink_metadata() {
                            Err(_) => true,
                            Ok(dst_meta) => {
                                if dst_meta.file_type().is_dir() {
                                    fs::remove_dir_all(&target)?;
                                    true
                                } else if dst_meta.file_type().is_symlink() {
                                    fs::remove_file(&target)?;
                                    true
                                } else {
                                    needs_copy(&meta, &dst_meta, config)
                                }
                            }
                        };
                        if should_copy {
                            if let Ok(dst_meta) = target.symlink_metadata() {
                                if dst_meta.is_file() && dst_meta.mode() & 0o200 == 0 {
                                    let mut perms = dst_meta.permissions();
                                    perms.set_mode(dst_meta.mode() | 0o200);
                                    fs::set_permissions(&target, perms)?;
                                }
                            }
                            debug!("Sync: copy (hard-link first) {from:?} -> {target:?}");
                            stats.send(StatusUpdate::Size(meta.len()))?;
                            let hdl = CopyHandle::new(&from, &target, config)?;
                            hdl.copy_file(&stats)?;
                        } else {
                            if config.copy_xattrs {
                                if let Err(e) = sync_xattrs(&from, &target) {
                                    warn!("Failed to sync xattrs {from:?}: {e}");
                                }
                            }
                            debug!("Sync: skip unchanged (hard-link first) {from:?}");
                        }
                    }
                } else {
                    // Regular file (no hard-link preservation).
                    let should_copy = match target.symlink_metadata() {
                        Err(_) => true,
                        Ok(dst_meta) => {
                            if dst_meta.file_type().is_dir() {
                                fs::remove_dir_all(&target)?;
                                true
                            } else if dst_meta.file_type().is_symlink() {
                                fs::remove_file(&target)?;
                                true
                            } else {
                                needs_copy(&meta, &dst_meta, config)
                            }
                        }
                    };
                    if should_copy {
                        // If the existing destination file is not writable,
                        // add the owner write bit so File::create() can
                        // overwrite it. copy_permissions() will restore the
                        // correct permissions once the copy is complete.
                        if let Ok(dst_meta) = target.symlink_metadata() {
                            if dst_meta.is_file() && dst_meta.mode() & 0o200 == 0 {
                                let mut perms = dst_meta.permissions();
                                perms.set_mode(dst_meta.mode() | 0o200);
                                fs::set_permissions(&target, perms)?;
                            }
                        }
                        debug!("Sync: copy {from:?} -> {target:?}");
                        stats.send(StatusUpdate::Size(meta.len()))?;
                        work_tx.send(Operation::Copy(from, target))?;
                    } else {
                        if config.copy_xattrs {
                            if let Err(e) = sync_xattrs(&from, &target) {
                                warn!("Failed to sync xattrs {from:?}: {e}");
                            }
                        }
                        debug!("Sync: skip unchanged {from:?}");
                    }
                }
            }

            FileType::Symlink => {
                let link_target = read_link(&from)?;
                let should_link = match target.symlink_metadata() {
                    Err(_) => true,
                    Ok(dst_meta) => {
                        if !dst_meta.file_type().is_symlink() {
                            true
                        } else {
                            read_link(&target)? != link_target
                        }
                    }
                };
                if should_link {
                    if target.symlink_metadata().is_ok() {
                        if target.symlink_metadata().map(|m| m.file_type().is_dir()).unwrap_or(false) {
                            fs::remove_dir_all(&target)?;
                        } else {
                            fs::remove_file(&target)?;
                        }
                    }
                    debug!("Sync: symlink {link_target:?} -> {target:?}");
                    work_tx.send(Operation::Link(link_target, target))?;
                } else {
                    debug!("Sync: skip unchanged symlink {from:?}");
                }
            }

            FileType::Socket | FileType::Char | FileType::Fifo => {
                if config.copy_special {
                    debug!("Sync: special file {from:?} -> {target:?}");
                    work_tx.send(Operation::Special(from, target))?;
                } else {
                    debug!("Sync: skip special file {from:?}");
                }
            }

            FileType::Block => {
                if config.copy_special {
                    debug!("Sync: block device {from:?} -> {target:?}");
                    work_tx.send(Operation::Special(from, target))?;
                } else {
                    debug!("Sync: skip block device {from:?}");
                }
            }

            FileType::Other => {
                error!("Unsupported filetype found: {target:?} -> {ft:?}");
                return Err(XcpError::UnknownFileType(target).into());
            }
        }
    }

    // Delete destination entries absent from source. Collect all such paths,
    // then identify the subtree roots — paths whose parent is not also being
    // deleted — and remove each root in parallel using remove_dir_all.
    // This avoids the contents-first ordering constraint and lets multiple
    // independent subtrees be removed concurrently.
    if dest.is_dir() {
        let all_to_delete: Vec<PathBuf> = WalkDir::new(dest)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let rel = match e.path().strip_prefix(dest) {
                    Ok(r) => r,
                    Err(_) => return false,
                };
                !empty_path(rel) && !source_relpaths.contains(rel)
            })
            .map(|e| e.into_path())
            .collect();

        if !all_to_delete.is_empty() {
            // Keep only the top-most paths so remove_dir_all handles subtrees.
            let delete_set: HashSet<&Path> = all_to_delete.iter().map(PathBuf::as_path).collect();
            let roots: Vec<PathBuf> = all_to_delete
                .iter()
                .filter(|p| {
                    p.parent()
                        .map(|parent| !delete_set.contains(parent))
                        .unwrap_or(true)
                })
                .cloned()
                .collect();

            parallel_delete(roots, config.num_workers())?;
        }
    }

    debug!("Sync walk-worker finished: {:?}", thread::current().id());
    Ok(())
}

/// Delete `roots` in parallel across `nworkers` threads. Each root is removed
/// with `remove_dir_all` (directories) or `remove_file` (files/symlinks), so
/// callers must pass only subtree roots — not individual descendants.
fn parallel_delete(roots: Vec<PathBuf>, nworkers: usize) -> Result<()> {
    let (tx, rx) = cbc::unbounded::<PathBuf>();

    let handles: Vec<_> = (0..nworkers)
        .map(|_| {
            let wrx = rx.clone();
            thread::spawn(move || -> Result<()> {
                for path in wrx {
                    debug!("Sync: delete {path:?}");
                    match path.symlink_metadata() {
                        Err(_) => {} // already gone
                        Ok(m) if m.is_dir() => fs::remove_dir_all(&path)?,
                        Ok(_) => fs::remove_file(&path)?,
                    }
                }
                Ok(())
            })
        })
        .collect();

    for path in roots {
        tx.send(path)
            .map_err(|_| XcpError::CopyError("Delete worker disconnected".to_string()))?;
    }
    drop(tx);

    for handle in handles {
        handle.join()
            .map_err(|_| XcpError::CopyError("Error in parallel delete".to_string()))??;
    }

    Ok(())
}
