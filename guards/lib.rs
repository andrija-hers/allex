//! allex-guards — resource headroom watchdogs for fire-and-forget services.
//!
//! A small, declarative way for any allex service to say *"watch this resource, and when it gets
//! tight, do this"* — without re-implementing free-space syscalls, level hysteresis and poll loops
//! per app.
//!
//! # Prevention vs. reaction (why disk and RAM differ)
//! Two resources, two strategies, and the difference is **responsiveness**:
//! - **Disk** fills slowly and leaves the box responsive, so detect-and-react works: a [`DiskGauge`]
//!   watchdog can trip a graceful retreat with room to spare.
//! - **RAM** is the opposite — once the OS is swapping, the reaction itself is paged out and too slow
//!   to fire. So RAM is *prevented* with static caps; a [`RamGauge`] watchdog is **defense-in-depth**,
//!   tripping at a *soft* line (e.g. 80% used) while the process is still healthy. It must never be
//!   relied on to catch true exhaustion — set its critical band well below physical RAM.
//!
//! # Three seams
//! - [`Gauge`] — measure headroom for one resource (the OS-specific part; one impl per resource).
//! - [`GuardPolicy`] — what to do on a level change (blanket-impl'd for closures, so a `Fn` works).
//! - [`Guard`] — the poll/hysteresis engine, blanket-impl'd for **every** [`Gauge`], so any gauge
//!   gets `.guard(thresholds, interval, policy)` for free.
//!
//! ```ignore
//! DiskGauge::new(store_path)
//!     .guard(Thresholds::reserve_mb(64, 256), Duration::from_secs(2),
//!            |t: &Transition| if t.to == Headroom::Critical { retreat("low disk") });
//!
//! RamGauge::system() // the box-wide "about to swap" line
//!     .guard(Thresholds::max_usage(0.80, 0.75, 0.03), Duration::from_secs(2),
//!            |t: &Transition| if t.to == Headroom::Critical { shed_load() });
//! ```

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Graduated headroom level for a guarded resource. Worsening (`Ok→Low→Critical`) is immediate;
/// recovery requires clearing the band by the configured hysteresis (see [`Thresholds`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Headroom {
    /// Comfortable.
    Ok,
    /// Getting tight — warn / shed growth (stop accepting new work).
    Low,
    /// Act now, while still responsive — retreat / shed load / controlled restart.
    Critical,
}

/// One measurement of a resource: how much headroom remains before its limit, and the total against
/// which a fractional threshold is judged. Disk: `headroom_bytes` = free space, `total_bytes` = volume
/// size. RAM(system): `headroom_bytes` = free, `total_bytes` = physical. RAM(budget): `headroom_bytes`
/// = budget − RSS, `total_bytes` = budget.
#[derive(Clone, Copy, Debug)]
pub struct Sample {
    /// Bytes of headroom remaining before the limit. Lower = more pressure.
    pub headroom_bytes: u64,
    /// Total against which fractional thresholds are computed.
    pub total_bytes: u64,
}

/// The measurement seam — one concrete impl per resource. Cheap & synchronous (a `statvfs` / `/proc`
/// read), called by the [`Guard`] poll loop.
pub trait Gauge: Send + Sync + 'static {
    /// Take a reading. An `Err` (e.g. a transient stat failure) makes the watchdog hold its last
    /// level rather than false-trip.
    fn sample(&self) -> io::Result<Sample>;
    /// Short label for logs / readings (`"disk"`, `"ram"`).
    fn label(&self) -> &'static str;
}

/// Where the `Low`/`Critical` bands sit, with recovery hysteresis to stop flapping at a boundary.
/// Express in absolute bytes (disk: "reserve N MB") or as a fraction of total (RAM: "trip at 80%").
#[derive(Clone, Copy, Debug)]
pub enum Thresholds {
    /// Bands in absolute bytes of headroom. `critical < low`; `recover` is the extra headroom needed
    /// to step back down a level.
    Bytes {
        low: u64,
        critical: u64,
        recover: u64,
    },
    /// Bands as a fraction of `total_bytes` of headroom (resolved per-sample).
    Fraction {
        low: f64,
        critical: f64,
        recover: f64,
    },
}

impl Thresholds {
    /// Disk-style: `Critical` when free headroom ≤ `critical_mb`, `Low` when ≤ `low_mb`. 16 MiB
    /// recovery hysteresis.
    pub fn reserve_mb(critical_mb: u64, low_mb: u64) -> Self {
        Thresholds::Bytes {
            critical: critical_mb << 20,
            low: low_mb << 20,
            recover: 16 << 20,
        }
    }

    /// RAM-style: trip on **usage** crossing fractions (e.g. `max_usage(0.80, 0.75, 0.03)` → Critical
    /// at 80% used, Low at 75% used). Converts "used" to "headroom remaining" (`1 − used`).
    pub fn max_usage(critical_used: f64, low_used: f64, recover: f64) -> Self {
        Thresholds::Fraction {
            critical: 1.0 - critical_used,
            low: 1.0 - low_used,
            recover,
        }
    }

    /// Resolve to absolute `(low, critical, recover)` headroom bytes for a sample's `total`.
    fn bands(&self, total: u64) -> (u64, u64, u64) {
        match *self {
            Thresholds::Bytes {
                low,
                critical,
                recover,
            } => (low, critical, recover),
            Thresholds::Fraction {
                low,
                critical,
                recover,
            } => {
                let f = total as f64;
                // round, not truncate — `1.0 - 0.80` is `0.199999…` in f64, which would truncate a
                // clean 0.20 band to one byte short.
                (
                    (low * f).round() as u64,
                    (critical * f).round() as u64,
                    (recover * f).round() as u64,
                )
            }
        }
    }
}

/// Re-classify headroom given the previous level. Worsening is immediate; improving requires the
/// headroom to clear the band boundary by `recover` (hysteresis), so a value hovering at a threshold
/// can't flap.
fn reclassify(prev: Headroom, headroom: u64, low: u64, critical: u64, recover: u64) -> Headroom {
    match prev {
        Headroom::Ok => {
            if headroom <= critical {
                Headroom::Critical
            } else if headroom <= low {
                Headroom::Low
            } else {
                Headroom::Ok
            }
        }
        Headroom::Low => {
            if headroom <= critical {
                Headroom::Critical
            } else if headroom >= low.saturating_add(recover) {
                Headroom::Ok
            } else {
                Headroom::Low
            }
        }
        Headroom::Critical => {
            if headroom >= low.saturating_add(recover) {
                Headroom::Ok
            } else if headroom >= critical.saturating_add(recover) {
                Headroom::Low
            } else {
                Headroom::Critical
            }
        }
    }
}

/// A live reading published by a running guard.
#[derive(Clone, Copy, Debug)]
pub struct Reading {
    /// Current level.
    pub level: Headroom,
    /// Headroom remaining at the last poll.
    pub headroom_bytes: u64,
    /// The gauge's label.
    pub label: &'static str,
}

/// A level change, handed to the [`GuardPolicy`].
#[derive(Clone, Copy, Debug)]
pub struct Transition {
    /// Level before.
    pub from: Headroom,
    /// Level after.
    pub to: Headroom,
    /// The reading that triggered it.
    pub reading: Reading,
}

/// The reaction seam — what an app does when a guarded resource changes level. Blanket-impl'd for
/// closures, so `|t: &Transition| { … }` is a policy; a struct may also implement it directly.
pub trait GuardPolicy: Send + Sync + 'static {
    /// Called once per level change (not per poll).
    fn on_transition(&self, t: &Transition);
}

impl<F: Fn(&Transition) + Send + Sync + 'static> GuardPolicy for F {
    fn on_transition(&self, t: &Transition) {
        self(t)
    }
}

/// Handle to a running guard. Exposes the latest [`Reading`] for synchronous point-checks (e.g. a
/// "headroom OK right now?" skip-guard) and a [`watch`] receiver to react to changes. Aborts the
/// poll task on drop.
pub struct GuardHandle {
    rx: watch::Receiver<Reading>,
    task: JoinHandle<()>,
}

impl GuardHandle {
    /// The most recent reading (cheap; no syscall).
    pub fn current(&self) -> Reading {
        *self.rx.borrow()
    }

    /// A receiver for level updates — `changed().await` then `borrow()`.
    pub fn watch(&self) -> watch::Receiver<Reading> {
        self.rx.clone()
    }
}

impl Drop for GuardHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The engine: blanket-impl'd for every [`Gauge`], so `gauge.guard(thresholds, interval, policy)`
/// spawns a poll loop that publishes readings and fires `policy` on each level transition.
pub trait Guard: Gauge + Sized {
    /// Spawn the watchdog. Requires a tokio runtime. The returned [`GuardHandle`] stops the loop when
    /// dropped.
    fn guard(
        self,
        thresholds: Thresholds,
        every: Duration,
        policy: impl GuardPolicy,
    ) -> GuardHandle;
}

impl<G: Gauge> Guard for G {
    fn guard(
        self,
        thresholds: Thresholds,
        every: Duration,
        policy: impl GuardPolicy,
    ) -> GuardHandle {
        // Seed an accurate initial reading so `current()` is meaningful immediately (optimistic Ok if
        // the first sample errors — the first tick corrects it).
        let seed = self.sample().ok();
        let (seed_reading, mut level) = match seed {
            Some(s) => {
                let (low, crit, rec) = thresholds.bands(s.total_bytes);
                let lvl = reclassify(Headroom::Ok, s.headroom_bytes, low, crit, rec);
                (
                    Reading {
                        level: lvl,
                        headroom_bytes: s.headroom_bytes,
                        label: self.label(),
                    },
                    lvl,
                )
            }
            None => (
                Reading {
                    level: Headroom::Ok,
                    headroom_bytes: u64::MAX,
                    label: self.label(),
                },
                Headroom::Ok,
            ),
        };
        let (tx, rx) = watch::channel(seed_reading);

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(every);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Ok(s) = self.sample() else {
                    // Hold the last level on a transient read error rather than false-trip.
                    continue;
                };
                let (low, crit, rec) = thresholds.bands(s.total_bytes);
                let next = reclassify(level, s.headroom_bytes, low, crit, rec);
                let reading = Reading {
                    level: next,
                    headroom_bytes: s.headroom_bytes,
                    label: self.label(),
                };
                let _ = tx.send(reading);
                if next != level {
                    policy.on_transition(&Transition {
                        from: level,
                        to: next,
                        reading,
                    });
                    level = next;
                }
            }
        });

        GuardHandle { rx, task }
    }
}

// ---- gauges -------------------------------------------------------------------------------------

/// Watches free space on the volume holding `path`. Headroom = free bytes.
pub struct DiskGauge {
    path: PathBuf,
}

impl DiskGauge {
    /// Watch the filesystem containing `path` (the path must exist).
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl Gauge for DiskGauge {
    fn sample(&self) -> io::Result<Sample> {
        let (free, total) = platform::disk_free_total(&self.path)?;
        Ok(Sample {
            headroom_bytes: free,
            total_bytes: total,
        })
    }
    fn label(&self) -> &'static str {
        "disk"
    }
}

/// Watches memory. Two modes:
/// - [`RamGauge::system`] — box-wide free vs physical (the "about to swap" line; pair with
///   [`Thresholds::max_usage`]).
/// - [`RamGauge::budget`] — *this process'* RSS against a self-imposed budget (catches our own drift /
///   a mis-sized cap).
pub struct RamGauge {
    mode: RamMode,
}

#[derive(Clone, Copy)]
enum RamMode {
    System,
    Budget(u64),
}

impl RamGauge {
    /// Box-wide: headroom = available system memory, total = physical memory.
    pub fn system() -> Self {
        Self {
            mode: RamMode::System,
        }
    }
    /// Process-local: headroom = `budget_bytes − our RSS`, total = `budget_bytes`.
    pub fn budget(budget_bytes: u64) -> Self {
        Self {
            mode: RamMode::Budget(budget_bytes),
        }
    }
}

impl Gauge for RamGauge {
    fn sample(&self) -> io::Result<Sample> {
        match self.mode {
            RamMode::System => {
                let (avail, total) = platform::system_mem_avail_total()?;
                Ok(Sample {
                    headroom_bytes: avail,
                    total_bytes: total,
                })
            }
            RamMode::Budget(b) => {
                let rss = platform::process_rss()?;
                Ok(Sample {
                    headroom_bytes: b.saturating_sub(rss),
                    total_bytes: b,
                })
            }
        }
    }
    fn label(&self) -> &'static str {
        "ram"
    }
}

// ---- platform measurement -----------------------------------------------------------------------

#[cfg(unix)]
mod platform {
    use std::io;
    use std::path::Path;

    /// `(free_bytes_available_to_unprivileged, total_bytes)` for the filesystem holding `path`.
    pub fn disk_free_total(path: &Path) -> io::Result<(u64, u64)> {
        use std::os::unix::ffi::OsStrExt;
        let c = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
        // SAFETY: `c` is a valid NUL-terminated path; `stat` is fully written by statvfs on success.
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(c.as_ptr(), &mut stat) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let frsize = stat.f_frsize as u64;
        Ok((
            (stat.f_bavail as u64).saturating_mul(frsize),
            (stat.f_blocks as u64).saturating_mul(frsize),
        ))
    }

    /// `(available_bytes, total_bytes)` of system memory, from `/proc/meminfo`.
    pub fn system_mem_avail_total() -> io::Result<(u64, u64)> {
        let info = std::fs::read_to_string("/proc/meminfo")?;
        let kb = |key: &str| -> Option<u64> {
            info.lines()
                .find(|l| l.starts_with(key))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        };
        let total = kb("MemTotal:")
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "MemTotal missing"))?;
        let avail = kb("MemAvailable:")
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "MemAvailable missing"))?;
        Ok((avail, total))
    }

    /// Resident set size of this process, from `/proc/self/statm` (resident pages × page size).
    pub fn process_rss() -> io::Result<u64> {
        let statm = std::fs::read_to_string("/proc/self/statm")?;
        let resident: u64 = statm
            .split_whitespace()
            .nth(1)
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "statm resident missing"))?;
        // SAFETY: sysconf with a static argument is always safe.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
        Ok(resident.saturating_mul(page))
    }
}

#[cfg(windows)]
mod platform {
    use std::io;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;

    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
    use windows_sys::Win32::System::ProcessStatus::{
        K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    /// `(free_bytes_available_to_caller, total_bytes)` for the volume holding `path`, via
    /// `GetDiskFreeSpaceExW`. The "available to caller" figure honours per-user quotas, mirroring the
    /// Unix `f_bavail` (available to an unprivileged process) — so the disk gauge means the same thing
    /// on both platforms.
    pub fn disk_free_total(path: &Path) -> io::Result<(u64, u64)> {
        // GetDiskFreeSpaceExW resolves free space for a *directory*; handed a file path (e.g. the redb
        // store file) it fails. Resolve to the containing directory when `path` isn't one itself, so
        // the gauge keeps the Unix `statvfs(path)` contract of "the volume containing `path`".
        let dir = if path.is_dir() {
            path
        } else {
            match path.parent() {
                Some(p) if !p.as_os_str().is_empty() => p,
                _ => Path::new("."),
            }
        };
        // Pass the directory as a NUL-terminated wide string.
        let mut wide: Vec<u16> = dir.as_os_str().encode_wide().collect();
        wide.push(0);
        let mut free_to_caller: u64 = 0;
        let mut total: u64 = 0;
        // SAFETY: `wide` is a valid NUL-terminated UTF-16 path; the out-pointers are valid for the
        // call and the two we read are fully written on success (the third — total free — is unused).
        let ok = unsafe {
            GetDiskFreeSpaceExW(
                wide.as_ptr(),
                &mut free_to_caller,
                &mut total,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((free_to_caller, total))
    }

    /// `(available_bytes, total_bytes)` of physical memory, via `GlobalMemoryStatusEx`. `ullAvailPhys`
    /// is the box-wide "about to swap" headroom; `ullTotalPhys` is installed physical RAM. Mirrors
    /// Linux `MemAvailable` / `MemTotal`.
    pub fn system_mem_avail_total() -> io::Result<(u64, u64)> {
        // SAFETY: a zeroed MEMORYSTATUSEX with `dwLength` set is the documented input; the kernel
        // fully writes the struct on success.
        let mut status: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
        status.dwLength = std::mem::size_of::<MEMORYSTATUSEX>() as u32;
        let ok = unsafe { GlobalMemoryStatusEx(&mut status) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((status.ullAvailPhys, status.ullTotalPhys))
    }

    /// Resident set size of this process = the Win32 **working set** (`WorkingSetSize` from
    /// `GetProcessMemoryInfo`), the closest analogue to Unix RSS. We use the kernel32-exported
    /// `K32GetProcessMemoryInfo` so no separate `psapi.dll` link is needed.
    pub fn process_rss() -> io::Result<u64> {
        // SAFETY: a zeroed PROCESS_MEMORY_COUNTERS with `cb` set is the documented input;
        // `GetCurrentProcess` returns a pseudo-handle that needs no close; the struct is fully
        // written on success.
        let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
        counters.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
        let ok =
            unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters.cb) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(counters.WorkingSetSize as u64)
    }
}

#[cfg(not(any(unix, windows)))]
mod platform {
    use std::io;
    use std::path::Path;

    // Unix and Windows are implemented above; any other target (e.g. wasm) errors per-sample, which
    // makes the watchdog hold `Ok` (it never false-trips a retreat).
    fn unsupported<T>() -> io::Result<T> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "allex-guards: resource measurement not implemented on this platform",
        ))
    }
    pub fn disk_free_total(_path: &Path) -> io::Result<(u64, u64)> {
        unsupported()
    }
    pub fn system_mem_avail_total() -> io::Result<(u64, u64)> {
        unsupported()
    }
    pub fn process_rss() -> io::Result<u64> {
        unsupported()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn hysteresis_worsens_immediately_recovers_with_margin() {
        // bands: low=100, critical=50, recover=20
        let (low, crit, rec) = (100u64, 50u64, 20u64);
        // Worsening is immediate.
        assert_eq!(reclassify(Headroom::Ok, 120, low, crit, rec), Headroom::Ok);
        assert_eq!(reclassify(Headroom::Ok, 90, low, crit, rec), Headroom::Low);
        assert_eq!(
            reclassify(Headroom::Ok, 40, low, crit, rec),
            Headroom::Critical
        );
        assert_eq!(
            reclassify(Headroom::Low, 40, low, crit, rec),
            Headroom::Critical
        );
        // Recovery needs margin: at headroom 110 (just above low) we stay Low until low+recover=120.
        assert_eq!(
            reclassify(Headroom::Low, 110, low, crit, rec),
            Headroom::Low
        );
        assert_eq!(reclassify(Headroom::Low, 120, low, crit, rec), Headroom::Ok);
        // From Critical: 60 (>crit but <crit+recover=70) stays Critical; 70 steps to Low; 120 to Ok.
        assert_eq!(
            reclassify(Headroom::Critical, 60, low, crit, rec),
            Headroom::Critical
        );
        assert_eq!(
            reclassify(Headroom::Critical, 70, low, crit, rec),
            Headroom::Low
        );
        assert_eq!(
            reclassify(Headroom::Critical, 120, low, crit, rec),
            Headroom::Ok
        );
    }

    #[test]
    fn fractional_bands_resolve_against_total() {
        // max_usage(0.80, 0.75, 0.03): headroom fractions critical=0.20, low=0.25, recover=0.03.
        let t = Thresholds::max_usage(0.80, 0.75, 0.03);
        let (low, crit, rec) = t.bands(1000);
        assert_eq!((low, crit, rec), (250, 200, 30));
    }

    #[test]
    fn real_gauges_read() {
        // Disk on the crate dir, system RAM, and a budget — all should sample without error on every
        // platform with a `platform` impl (Unix via statvfs/proc, Windows via the Win32 calls).
        let disk = DiskGauge::new(".").sample().unwrap();
        assert!(disk.total_bytes > 0 && disk.headroom_bytes > 0);
        // A *file* path must also resolve (Windows `GetDiskFreeSpaceExW` needs a directory, so the
        // gauge resolves a file to its parent; Unix `statvfs` takes either). `Cargo.toml` always exists.
        let disk_file = DiskGauge::new("Cargo.toml").sample().unwrap();
        assert!(disk_file.total_bytes > 0);
        let ram = RamGauge::system().sample().unwrap();
        assert!(ram.total_bytes > 0);
        let budget = RamGauge::budget(64 << 20).sample().unwrap();
        assert_eq!(budget.total_bytes, 64 << 20);
    }

    /// A gauge whose headroom the test drives, to exercise the engine deterministically.
    struct TestGauge(Arc<AtomicU64>);
    impl Gauge for TestGauge {
        fn sample(&self) -> io::Result<Sample> {
            Ok(Sample {
                headroom_bytes: self.0.load(Ordering::SeqCst),
                total_bytes: 1000,
            })
        }
        fn label(&self) -> &'static str {
            "test"
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn engine_fires_transitions_and_tracks_level() {
        let h = Arc::new(AtomicU64::new(1000)); // start comfortable
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(Headroom, Headroom)>();
        let guard = TestGauge(h.clone()).guard(
            Thresholds::Bytes {
                low: 300,
                critical: 100,
                recover: 50,
            },
            Duration::from_millis(5),
            move |t: &Transition| {
                let _ = tx.send((t.from, t.to));
            },
        );

        // Drop into Critical.
        h.store(80, Ordering::SeqCst);
        let (from, to) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(from, Headroom::Ok);
        assert_eq!(to, Headroom::Critical);
        assert_eq!(guard.current().level, Headroom::Critical);

        // Recover well above low+recover → back to Ok.
        h.store(900, Ordering::SeqCst);
        let (from, to) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(from, Headroom::Critical);
        assert_eq!(to, Headroom::Ok);
    }
}
