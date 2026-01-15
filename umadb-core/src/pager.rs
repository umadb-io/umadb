use crate::common::PageID;
use memmap2::{Mmap, MmapOptions};
// use memmap2::{Advice, MmapOptions};
use nix::fcntl;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, RwLock};
use umadb_dcb::{DCBError, DCBResult};

// Pager for file I/O
pub struct Pager {
    pub reader: Arc<File>,
    pub writer: Arc<File>,
    pub writer_raw_fd: RawFd,
    pub page_size: usize,
    pub is_file_new: bool,
    // Number of logical database pages contained in a single mmap window.
    mmap_pages_per_map: usize,
    // Cache of memory maps, keyed by map identifier (floor(page_id / mmap_pages_per_map)).
    mmaps: RwLock<HashMap<u64, Arc<Mmap>>>,
    no_fsync: bool,
}

// Implementation for Pager
impl Pager {
    pub fn new(path: &Path, page_size: usize) -> DCBResult<Self> {
        let is_file_new = !path.exists();

        let reader_file = match if is_file_new {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
        } else {
            OpenOptions::new().read(true).write(true).open(path)
        } {
            Ok(reader_file) => reader_file,
            Err(err) => {
                return Err(DCBError::InitializationError(format!(
                    "Couldn't open file {} for reading: {}",
                    path.display(),
                    err
                )));
            }
        };

        let writer_file = match if is_file_new {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
        } else {
            OpenOptions::new().read(true).write(true).open(path)
        } {
            Ok(reader_file) => reader_file,
            Err(err) => {
                return Err(DCBError::InitializationError(format!(
                    "Couldn't open file {} for writing: {}",
                    path.display(),
                    err
                )));
            }
        };

        let writer_raw_fd = writer_file.as_raw_fd();

        // Compute pages per mmap so that:
        // - Each mmap offset is aligned to OS page size (and implicitly DB page size), and
        // - Each mapping window is at least 10 MiB.
        let os_ps = Self::os_page_size();
        let g = Self::gcd(os_ps, page_size);
        // Alignment factor: number of DB pages per window must be a multiple of this to keep
        // mmap offsets aligned to OS page boundaries.
        let align_pages = usize::max(1, os_ps / g);
        // Minimum window size in bytes: 10 MiB
        let min_window_bytes: usize = 256 * 1024 * 1024;
        // Minimum number of DB pages to reach at least the min window size
        let min_pages = min_window_bytes.div_ceil(page_size);
        // Choose smallest multiple of align_pages that is >= min_pages
        let k = if min_pages == 0 {
            1
        } else {
            min_pages.div_ceil(align_pages)
        };
        let mmap_pages_per_map = align_pages * usize::max(1, k);

        Ok(Self {
            reader: Arc::new(reader_file),
            writer: Arc::new(writer_file),
            writer_raw_fd,
            page_size,
            is_file_new,
            mmap_pages_per_map,
            mmaps: RwLock::new(HashMap::new()),
            // no_fsync: std::env::var("UMADB_NO_FSYNC").ok().is_some(),
            no_fsync: false,
        })
    }

    fn gcd(mut a: usize, mut b: usize) -> usize {
        while b != 0 {
            let t = b;
            b = a % t;
            a = t;
        }
        a
    }

    #[cfg(unix)]
    fn os_page_size() -> usize {
        // Safe: sysconf is thread-safe and returns a constant for page size
        unsafe {
            let sz = libc::sysconf(libc::_SC_PAGESIZE);
            if sz <= 0 {
                4096usize // sensible default
            } else {
                sz as usize
            }
        }
    }

    pub fn read_page(&self, page_id: PageID) -> io::Result<Vec<u8>> {
        let file = self.reader.clone();
        let offset = page_id.0 * (self.page_size as u64);
        let mut page = vec![0u8; self.page_size];
        let bytes_read = file.read_at(&mut page, offset)?;
        if bytes_read < self.page_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Page {page_id:?} not found"),
            ));
        }
        Ok(page)
    }

    // pub fn read_page_mmap(&self, page_id: PageID) -> io::Result<Vec<u8>> {
    //     // Precompute addressing values
    //     let page_size_u64 = self.page_size as u64;
    //     let offset = page_id.0 * page_size_u64;
    //     let pages_per_map = self.mmap_pages_per_map as u64;
    //     let map_id = page_id.0 / pages_per_map;
    //     let map_offset = map_id * pages_per_map * page_size_u64;
    //     let within = (offset - map_offset) as usize;
    //
    //     // Fast path: if mapping exists, do not obtain file lock; take Arc clone and release map lock
    //     if let Some(mmap_arc) = {
    //         let maps = self.mmaps.lock().unwrap();
    //         maps.get(&map_id).cloned()
    //     } {
    //         let start = within;
    //         let stop = start + self.page_size;
    //         if stop > mmap_arc.len() {
    //             return Err(io::Error::new(
    //                 io::ErrorKind::UnexpectedEof,
    //                 format!("Page {page_id:?} not found"),
    //             ));
    //         }
    //         return Ok(mmap_arc[start..stop].to_vec());
    //     }
    //
    //     // Slow path: need to create the mapping with double-checked locking
    //     let file = self.reader.lock().unwrap();
    //     let file_len = file.metadata()?.len();
    //
    //     // Re-check if mapping appeared while we acquired the file lock
    //     if let Some(mmap_arc) = {
    //         let maps = self.mmaps.lock().unwrap();
    //         maps.get(&map_id).cloned()
    //     } {
    //         let start = within;
    //         let stop = start + self.page_size;
    //         if stop > mmap_arc.len() {
    //             return Err(io::Error::new(
    //                 io::ErrorKind::UnexpectedEof,
    //                 format!("Page {page_id:?} not found"),
    //             ));
    //         }
    //         return Ok(mmap_arc[start..stop].to_vec());
    //     }
    //
    //     // Calculate standard mapping window length (does not depend on current file length)
    //     let max_len = pages_per_map * page_size_u64;
    //
    //     // Before mapping, ensure the requested page is within the current file length.
    //     let page_end = offset + page_size_u64;
    //     if page_end > file_len {
    //         return Err(io::Error::new(
    //             io::ErrorKind::UnexpectedEof,
    //             format!("Page {page_id:?} not found"),
    //         ));
    //     }
    //
    //     // Ensure the underlying file is large enough to permit a full standard-length mapping.
    //     let required_len = map_offset + max_len;
    //     if file_len < required_len {
    //         file.set_len(required_len)?;
    //     }
    //
    //     // Create the mmap and insert it, but guard with a double-check
    //     let mmap_new = unsafe {
    //         MmapOptions::new()
    //             .offset(map_offset)
    //             .len(max_len as usize)
    //             .map(&*file)?
    //     };
    //     let mmap_arc = {
    //         let mut maps = self.mmaps.lock().unwrap();
    //         // Another thread could have inserted meanwhile
    //         if let Some(existing) = maps.get(&map_id) {
    //             existing.clone()
    //         } else {
    //             let arc = Arc::new(mmap_new);
    //             maps.insert(map_id, arc.clone());
    //             // println!(
    //             //     "Created new mmap: map_id={} offset={} len={}",
    //             //     map_id, map_offset, max_len
    //             // );
    //             arc
    //         }
    //     };
    //
    //     // Now copy the requested page
    //     let start = within;
    //     let stop = start + self.page_size;
    //     if stop > mmap_arc.len() {
    //         return Err(io::Error::new(
    //             io::ErrorKind::UnexpectedEof,
    //             format!("Page {page_id:?} not found"),
    //         ));
    //     }
    //     Ok(mmap_arc[start..stop].to_vec())
    // }

    pub fn read_page_mmap_slice(&self, page_id: PageID) -> io::Result<MappedPage> {
        // Precompute addressing values
        let page_size_u64 = self.page_size as u64;
        let offset = page_id.0 * page_size_u64;
        let pages_per_map = self.mmap_pages_per_map as u64;
        let map_id = page_id.0 / pages_per_map;
        let map_offset = map_id * pages_per_map * page_size_u64;
        let within = (offset - map_offset) as usize;

        // Fast path: if mapping exists, do not obtain file lock; take Arc clone and release map lock
        if let Some(mmap_arc) = {
            let maps = self.mmaps.read().unwrap();
            maps.get(&map_id).cloned()
        } {
            let start = within;
            let stop = start + self.page_size;
            if stop > mmap_arc.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("Page {page_id:?} not found"),
                ));
            }
            return Ok(MappedPage {
                mmap: mmap_arc,
                start,
                len: self.page_size,
            });
        }

        // Slow path: need to create the mapping with double-checked locking
        let file = self.reader.clone();
        let file_len = file.metadata()?.len();

        // Re-check if mapping appeared while we acquired the file lock
        if let Some(mmap_arc) = {
            let maps = self.mmaps.read().unwrap();
            maps.get(&map_id).cloned()
        } {
            let start = within;
            let stop = start + self.page_size;
            if stop > mmap_arc.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("Page {page_id:?} not found"),
                ));
            }
            return Ok(MappedPage {
                mmap: mmap_arc,
                start,
                len: self.page_size,
            });
        }

        // Calculate standard mapping window length (does not depend on current file length)
        let max_len = pages_per_map * page_size_u64;

        // Before mapping, ensure the requested page is within the current file length.
        let page_end = offset + page_size_u64;
        if page_end > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Page {page_id:?} not found"),
            ));
        }

        // Ensure the underlying file is large enough to permit a full standard-length mapping.
        let required_len = map_offset + max_len;
        if file_len < required_len {
            file.set_len(required_len)?;
        }

        // Create the mmap and insert it, but guard with a double-check
        let mmap_new = unsafe {
            MmapOptions::new()
                .offset(map_offset)
                .len(max_len as usize)
                .map(&*file)?
        };
        // mmap_new.advise(Advice::Random)?;
        // mmap_new.advise(Advice::WillNeed)?;
        let mmap_arc = {
            let mut maps = self.mmaps.write().unwrap();
            // Another thread could have inserted meanwhile
            if let Some(existing) = maps.get(&map_id) {
                existing.clone()
            } else {
                let arc = Arc::new(mmap_new);
                maps.insert(map_id, arc.clone());
                // println!(
                //     "Created new mmap: map_id={} offset={} len={}",
                //     map_id, map_offset, max_len
                // );
                arc
            }
        };

        // Now form the mapped page view
        let start = within;
        let stop = start + self.page_size;
        if stop > mmap_arc.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Page {page_id:?} not found"),
            ));
        }
        Ok(MappedPage {
            mmap: mmap_arc,
            start,
            len: self.page_size,
        })
    }

    pub fn write_page(&self, page_id: PageID, page_data: &[u8]) -> DCBResult<()> {
        // Check the page doesn't overflow the page size.
        if page_data.len() != self.page_size {
            return Err(DCBError::InternalError(format!(
                "Page size mismatch: page_id={:?} size={} > PAGE_SIZE={}",
                page_id,
                page_data.len(),
                self.page_size
            )));
        }

        // Check the page doesn't overflow the file size.
        let file_len = self.writer.metadata()?.len();
        if self.page_size as u64 * (page_id.0 + 1) > file_len
            && let Err(err) = preallocate(
                &self.writer,
                (self.mmap_pages_per_map * self.page_size) as u64,
            )
        {
            return Err(DCBError::Io(err));
        }

        // Write the page data
        self.writer
            .write_at(page_data, page_id.0 * (self.page_size as u64))?;

        Ok(())
    }

    pub fn fsync(&self) -> io::Result<()> {
        if self.no_fsync {
            return Ok(());
        }

        #[cfg(target_os = "macos")]
        unsafe {
            let result = libc::fsync(self.writer_raw_fd);
            if result != 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        #[cfg(target_os = "linux")]
        {
            // On Linux, prefer sync_data() over fsync() because:
            // - It avoids unnecessary metadata flushes
            // - It is implemented as fdatasync(), which is the Linux idiom
            self.writer.sync_data()
        }

        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "fsync not supported on this platform",
            ))
        }
    }

    #[cfg(test)]
    pub fn debug_mmap_count(&self) -> usize {
        self.mmaps.read().unwrap().len()
    }

    #[cfg(test)]
    pub fn debug_pages_per_mmap(&self) -> usize {
        self.mmap_pages_per_map
    }
}

// A zero-copy view over a page backed by a memory map. Holds an Arc to keep the mapping alive.
#[derive(Debug)]
pub struct MappedPage {
    mmap: Arc<Mmap>,
    start: usize,
    len: usize,
}

impl MappedPage {
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[self.start..self.start + self.len]
    }
}

/// Preallocate `len` bytes *beyond the current file size*.
///
/// - Linux: uses `posix_fallocate` (guaranteed allocation)
/// - macOS: uses `F_PREALLOCATE` (contiguous first, then non-contiguous)
/// - Other OS: falls back to `File::set_len()` (may be sparse)
///
/// Returns an error if allocation failed.
pub fn preallocate(file: &File, len: u64) -> io::Result<()> {
    let current_len = file.metadata()?.len();

    // --- LINUX ---------------------------------------------------------------
    #[cfg(target_os = "linux")]
    {
        current_len
            .checked_add(len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "size overflow"))?;

        let offset = current_len as i64;
        let length = len as i64;

        match fcntl::posix_fallocate(file, offset, length) {
            Ok(()) => return Ok(()),
            Err(e) => {
                return Err(io::Error::from_raw_os_error(e as i32));
            }
        }
    }

    // --- macOS ---------------------------------------------------------------
    #[cfg(target_os = "macos")]
    {
        use nix::libc::{self, F_ALLOCATEALL, F_ALLOCATECONTIG, F_PEOFPOSMODE, fstore_t};

        let new_len = current_len
            .checked_add(len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "size overflow"))?;

        let mut store: fstore_t = fstore_t {
            fst_flags: F_ALLOCATECONTIG,
            fst_posmode: F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: len as i64,
            fst_bytesalloc: 0,
        };

        // Try contiguous allocation
        let r = fcntl::fcntl(
            file, // <-- MUST BE the File object, not `fd`
            fcntl::FcntlArg::F_PREALLOCATE(&mut store),
        );
        if r.is_err() {
            println!("Trying non-contiguous file allocation");
            // Try non-contiguous allocation
            let mut store2: fstore_t = fstore_t {
                fst_flags: F_ALLOCATEALL,
                fst_posmode: F_PEOFPOSMODE,
                fst_offset: 0,
                fst_length: len as i64,
                fst_bytesalloc: 0,
            };

            let r2 = fcntl::fcntl(file, fcntl::FcntlArg::F_PREALLOCATE(&mut store2));

            if r2.is_err() {
                // fallback
                file.set_len(new_len)?;
                return Ok(());
            }
            // } else {
            //     println!("Success with contiguous file allocation");
        }

        // Now extend file size (macOS requirement)
        let fd = file.as_raw_fd();
        unsafe {
            if libc::ftruncate(fd, new_len as i64) != 0 {
                return Err(io::Error::last_os_error());
            }
        }

        Ok(())
    }

    // --- OTHER OSes ----------------------------------------------------------
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        file.set_len(new_len)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Pager;
    use crate::common::PageID;
    use std::path::PathBuf;
    use tempfile::tempdir;
    use umadb_dcb::DCBError;

    fn temp_file_path(name: &str) -> PathBuf {
        let dir = tempdir().expect("tempdir");
        dir.keep().join(name)
    }

    #[test]
    fn test_write_page_size_mismatch_error() {
        let page_size = 1024usize;
        let path = temp_file_path("pager_mmap_test.db");
        let pager = Pager::new(&path, page_size).expect("pager new");

        let data1 = vec![1u8; 100];

        let err = pager.write_page(PageID(0), &data1);
        assert!(matches!(err, Err(DCBError::InternalError(_))));
    }

    #[test]
    fn mmap_read_matches_normal_read() {
        let page_size = 1024usize;
        let path = temp_file_path("pager_mmap_test.db");
        let pager = Pager::new(&path, page_size).expect("pager new");

        let data1 = vec![1u8; page_size];
        let data2 = (0..page_size).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        pager.write_page(PageID(0), &data1).expect("write page 0");
        pager.write_page(PageID(1), &data2).expect("write page 1");
        pager.fsync().expect("couldn't fsync");

        let r0 = pager.read_page(PageID(0)).expect("read0");
        let r0m = pager.read_page_mmap_slice(PageID(0)).expect("read0m");
        assert_eq!(
            r0,
            r0m.as_slice(),
            "mmap read should match std read for page 0"
        );

        let r1 = pager.read_page(PageID(1)).expect("read1");
        let r1m = pager.read_page_mmap_slice(PageID(1)).expect("read1m");
        assert_eq!(
            r1,
            r1m.as_slice(),
            "mmap read should match std read for page 1"
        );
        assert_eq!(&r1[..], &data2[..]);
    }

    #[test]
    fn mmap_read_out_of_bounds() {
        let page_size = 512usize;
        let path = temp_file_path("pager_mmap_oob.db");
        let pager = Pager::new(&path, page_size).expect("pager new");

        let buf = vec![0u8; page_size];
        pager.write_page(PageID(0), &buf).expect("write p0");
        pager.fsync().expect("couldn't fsync");

        let err = pager
            .read_page_mmap_slice(PageID(256 * 1024 * 2))
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn mmap_reuse_within_same_map() {
        // Try a few page sizes to ensure we get at least 2 pages per mmap window
        let candidates = [512usize, 1024, 2048, 4096, 8192];
        let mut pager_opt = None;
        for ps in candidates {
            let path = temp_file_path("pager_mmap_reuse.db");
            let pager = Pager::new(&path, ps).expect("pager new");
            if pager.debug_pages_per_mmap() >= 2 {
                pager_opt = Some((pager, ps));
                break;
            }
        }
        let (pager, page_size) = pager_opt.expect("could not find suitable page size for test");

        // Write two pages
        pager
            .write_page(PageID(0), &vec![1u8; page_size])
            .expect("write p0");
        pager
            .write_page(PageID(1), &vec![2u8; page_size])
            .expect("write p1");
        pager.fsync().expect("couldn't fsync");

        // Read both pages via mmap
        let _ = pager.read_page_mmap_slice(PageID(0)).expect("read0m");
        assert_eq!(pager.debug_mmap_count(), 1, "first mmap created");
        let _ = pager.read_page_mmap_slice(PageID(1)).expect("read1m");
        assert_eq!(
            pager.debug_mmap_count(),
            1,
            "should reuse same mmap for pages in same window"
        );
    }

    #[test]
    fn mmap_creates_new_on_boundary() {
        // Ensure pages_per_mmap is available and >= 2 by selecting suitable page size
        let candidates = [512usize, 1024, 2048, 4096, 8192];
        let mut pager_opt = None;
        for ps in candidates {
            let path = temp_file_path("pager_mmap_boundary.db");
            let pager = Pager::new(&path, ps).expect("pager new");
            if pager.debug_pages_per_mmap() >= 1 {
                pager_opt = Some((pager, ps));
                break;
            }
        }
        let (pager, page_size) = pager_opt.expect("failed to create pager");
        let ppm = pager.debug_pages_per_mmap();

        // Write enough pages to cover two windows
        for p in 0..(ppm as u64 + 1) {
            let fill = if (p % 2) == 0 { 0xAA } else { 0x55 };
            pager
                .write_page(PageID(p), &vec![fill; page_size])
                .expect("write page");
        }
        pager.fsync().expect("couldn't fsync");

        // First read creates first mmap
        let _ = pager
            .read_page_mmap_slice(PageID(0))
            .expect("read first window");
        assert_eq!(pager.debug_mmap_count(), 1);
        // Reading the first page in the next window should create a second mmap
        let _ = pager
            .read_page_mmap_slice(PageID(ppm as u64))
            .expect("read second window");
        assert_eq!(pager.debug_mmap_count(), 2);
    }
}
