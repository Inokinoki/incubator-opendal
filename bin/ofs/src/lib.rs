// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use fuse3::path::prelude::*;
use fuse3::{Errno, Result};

use async_trait::async_trait;
use futures::executor::block_on;
use futures_util::stream::{Empty, Iter};
use futures_util::{stream, StreamExt};
use std::ffi::OsStr;
use std::vec::IntoIter;

use opendal::EntryMode;
use opendal::Operator;

pub struct Ofs {
    pub op: Operator,
}

#[async_trait]
impl PathFilesystem for Ofs {
    type DirEntryStream = Iter<IntoIter<Result<DirectoryEntry>>>;
    type DirEntryPlusStream = Empty<Result<DirectoryEntryPlus>>;

    // Init a fuse filesystem
    async fn init(&self, _req: Request) -> Result<()> {
        Ok(())
    }

    // Callback when fs is being destroyed
    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<ReplyEntry> {
        // TODO
        log::debug!("lookup(parent={:?}, name={:?})", parent, name);
        Err(libc::ENOSYS.into())
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        // TODO
        log::debug!("getattr(path={:?})", path);

        Err(libc::ENOSYS.into())
    }

    async fn read(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        // TODO
        log::debug!(
            "read(path={:?}, fh={}, offset={}, size={})",
            path,
            fh,
            offset,
            size
        );

        Err(libc::ENOSYS.into())
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        // TODO
        log::debug!(
            "mkdir(parent={:?}, name={:?}, mode=0o{:o})",
            parent,
            name,
            mode
        );

        Err(libc::ENOSYS.into())
    }

    async fn readdir(
        &self,
        _req: Request,
        path: &OsStr,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream>> {
        log::debug!("readdir(path={:?}, fh={}, offset={})", path, fh, offset);

        let parent_path = path.to_string_lossy();

        // Check whether the parent is a dir
        match block_on(self.op.stat(&parent_path)) {
            Ok(parent_metadata) => {
                if parent_metadata.mode() != EntryMode::DIR {
                    // The parent is not a dir
                    return Err(Errno::new_is_not_dir());
                };
            }
            Err(_) => {
                return Err(Errno::new_not_exist());
            }
        };

        let entries = match block_on(self.op.list(&parent_path)) {
            Ok(entries) => entries,
            Err(error) => {
                log::warn!("readdir failed due to {:?}", error);
                return Err(Errno::new_not_exist());
            }
        };
        let mut directory_entries = Vec::new();
        if offset < 2 {
            match offset {
                0 => {
                    directory_entries.push(DirectoryEntry {
                        kind: FileType::Directory,
                        name: OsStr::new(".").into(),
                        offset: 0,
                    });
                    directory_entries.push(DirectoryEntry {
                        kind: FileType::Directory,
                        name: OsStr::new("..").into(),
                        offset: 1,
                    });
                }
                1 => {
                    directory_entries.push(DirectoryEntry {
                        kind: FileType::Directory,
                        name: OsStr::new("..").into(),
                        offset: 1,
                    });
                }
                _ => {}
            }
        }

        let mut inner_offset = 2;
        for (_, entry) in entries.into_iter().enumerate().skip((offset - 2) as usize) {
            let metadata = block_on(self.op.stat(entry.path())).unwrap();

            match metadata.mode() {
                EntryMode::FILE => {
                    log::debug!("Handling file");
                    directory_entries.push(DirectoryEntry {
                        kind: FileType::RegularFile,
                        name: entry.name().into(),
                        offset: inner_offset,
                    });

                    inner_offset = inner_offset + 1;
                }
                EntryMode::DIR => {
                    log::debug!("Handling dir {} {}", entry.path(), entry.name());

                    directory_entries.push(DirectoryEntry {
                        kind: FileType::Directory,
                        name: entry.name().into(),
                        offset: inner_offset,
                    });

                    inner_offset = inner_offset + 1;
                }
                EntryMode::Unknown => continue,
            };
        }

        Ok(ReplyDirectory {
            entries: stream::iter(block_on(
                stream::iter(directory_entries).map(Ok).collect::<Vec<_>>(),
            )),
        })
    }

    async fn mknod(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _rdev: u32,
    ) -> Result<ReplyEntry> {
        // TODO
        log::debug!(
            "mknod(parent={:?}, name={:?}, mode=0o{:o})",
            parent,
            name,
            mode
        );

        Err(libc::ENOSYS.into())
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> Result<ReplyOpen> {
        // TODO
        log::debug!("open(path={:?}, flags=0x{:x})", path, flags);

        Err(libc::ENOSYS.into())
    }

    async fn setattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        // TODO
        log::debug!("setattr(path={:?})", path);

        Err(libc::ENOSYS.into())
    }

    async fn write(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        data: &[u8],
        flags: u32,
    ) -> Result<ReplyWrite> {
        // TODO
        log::debug!(
            "write(path={:?}, fh={}, offset={}, len={}, flags=0x{:x})",
            path,
            fh,
            offset,
            data.len(),
            flags
        );

        Err(libc::ENOSYS.into())
    }

    async fn release(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        flags: u32,
        _lock_owner: u64,
        flush: bool,
    ) -> Result<()> {
        // TODO
        log::debug!(
            "release(path={:?}, fh={}, flags={}, flush={})",
            path,
            fh,
            flags,
            flush
        );

        Err(libc::ENOSYS.into())
    }

    async fn rename(
        &self,
        _req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<()> {
        // TODO
        log::debug!(
            "rename(p={:?}, name={:?}, newp={:?}, newname={:?})",
            origin_parent,
            origin_name,
            parent,
            name
        );

        Err(libc::ENOSYS.into())
    }

    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        // TODO
        log::debug!("unlink(parent={:?}, name={:?})", parent, name);

        Err(libc::ENOSYS.into())
    }
}
