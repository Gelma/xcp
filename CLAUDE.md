# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build
cargo build
cargo build --release

# Run all tests (auto-detects filesystem capabilities)
./tests/scripts/test-linux.sh

# Run tests manually (minimal)
cargo test --workspace

# Run a single test by name
cargo test --workspace test_name

# Run tests with specific driver
cargo test --workspace -- --test-filter parblock

# Run root-only tests
sudo ./tests/scripts/test-linux.sh test_run_root

# Run expensive tests
./tests/scripts/test-linux.sh test_run_expensive
```

The test script (`tests/scripts/test-linux.sh`) auto-detects the current filesystem and disables tests for unsupported features (reflinks on ext4, ACLs on vfat, etc.) via feature flags like `test_no_reflink`, `test_no_sparse`, etc.

## Workspace Structure

This is a Cargo workspace with three crates:

- **`xcp/`** (root) — the `xcp` binary: CLI parsing (`src/options.rs`), argument validation, glob expansion, progress bar rendering (`src/progress.rs`), and the main copy loop that threads the driver call and collects `StatusUpdate` messages.
- **`libxcp/`** — the copy engine library:
  - `config.rs` — `Config` struct (workers, block_size, reflink mode, backup mode, sync, etc.)
  - `drivers/` — pluggable driver trait (`CopyDriver`) with two implementations:
    - `parfile` (default): parallelises at the file level
    - `parblock` (feature-gated): parallelises at the block level
  - `feedback.rs` — `StatusUpdater` trait + `ChannelUpdater` (crossbeam channel) + `NoopUpdater`
  - `operations.rs` — per-file copy logic (`CopyHandle`, `tree_walker`, `sync_walker`)
  - `paths.rs` — source/destination path resolution
  - `backup.rs` — numbered backup file logic
- **`libfs/`** — low-level filesystem primitives:
  - `linux.rs` — Linux-specific: `copy_file_range`, sparse file detection (`probably_sparse`, `next_sparse_segments`), extent mapping (`map_extents`), reflink via ioctl
  - `fallback.rs` — non-Linux fallback implementations
  - `common.rs` — cross-platform: permissions, timestamps, ownership, xattrs, ACLs

## Architecture: Copy Flow

`main()` → validates args → builds `Arc<Config>` → `load_driver()` → spawns a thread calling `driver.copy(sources, dest, stats)` → main thread iterates `stat_rx` channel receiving `StatusUpdate::{Size, Copied, Error, Notice}` → updates progress bar or prints notice.

Drivers send `StatusUpdate` messages through the `StatusUpdater` trait; `ChannelUpdater` batches small `Copied` updates to avoid channel saturation (grouping by `block_size`).

## `--sync` mode

`xcp --sync src/ dst/` makes `dst/` identical to `src/` without rsync-style block deltas:

- **skip** files where mtime (nanosecond precision) + size + permissions all match
- **copy** files that are new or changed; if the destination file is read-only the owner write bit is temporarily added so `File::create` can overwrite it, then `copy_permissions` restores the correct mode
- **recreate** symlinks whose target changed
- **replace** entries whose type conflicts (source dir / dest file or vice versa)
- **delete** destination entries absent from the source: subtree roots are identified (paths whose parent is not also being deleted) and removed in parallel across `num_cpus` worker threads via `parallel_delete()`, each using `remove_dir_all`

Implemented in `libxcp/src/operations.rs` as `sync_walker()` and `parallel_delete()`, called by the `parfile` driver when `Config::sync` is true. Incompatible with `--no-clobber`; requires exactly one source directory.

### `--sync`-related flags

| Flag | Config field | Effect |
|---|---|---|
| `--hardlinks` | `preserve_hardlinks` | Detect shared inodes in source; recreate hard-link groups in dest. First occurrence is copied synchronously (walker thread) so subsequent links can be created immediately. Type conflicts at dest (symlink/dir where a link is expected) are removed first. |
| `--special` | `copy_special` | Sync special files (char/block devices, sockets, FIFOs). Without this flag, special files are skipped in `--sync`. Also enables block-device copying in normal mode. |
| `--xattrs` | `copy_xattrs` | Update extended attributes (incl. POSIX ACLs on Linux) for files that are otherwise unchanged. `sync_xattrs()` in `libfs` handles path-based xattr diff/apply. |
| `--sync-full` | (aggregate) | Implies `--sync --hardlinks --special --xattrs --ownership`. Handled in `Config::from(&Opts)`. |
| `--dry-run` | `dry_run` | Print every planned action (`would copy`, `would delete`, `would create dir`, etc.) without modifying any files. Actions are emitted as `StatusUpdate::Notice(String)` so the library stays I/O-free; the binary prints them. All write paths in `sync_walker` are gated on `!config.dry_run`; `parallel_delete` and `sync_dir_timestamps` are skipped entirely. Requires `--sync` or `--sync-full`. |

## Feature Flags

- `use_linux` — enables Linux-specific copy syscalls (`copy_file_range`, ioctl reflink); on by default
- `parblock` — enables the experimental block-parallel driver; on by default
- `test_no_*` flags — used only in tests to skip tests unsupported by the current filesystem
