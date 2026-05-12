# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### <!-- 1 -->Bug Fixes

- *(xcp)* `--sync` now correctly overwrites read-only destination files (e.g. `0444`); the owner write bit is temporarily added before the copy and the correct permissions are restored from the source afterwards.
- *(xcp)* `--sync` now correctly handles type conflicts: if a path is a directory in the source but a file/symlink in the destination (or vice versa), the destination entry is replaced with the correct type.

### <!-- 4 -->Performance

- *(xcp)* `--sync` deletion of stale destination entries is now parallelised across `num_cpus` worker threads; independent subtree roots are distributed via a channel and each removed with `remove_dir_all`, eliminating the previous sequential leaf-by-leaf pass.

### <!-- 0 -->Added

- *(xcp)* New `--sync` option to mirror a source directory onto a destination: copies new or changed files (compared by mtime + size + permissions), recreates symlinks whose target changed, and deletes destination entries absent from the source. Equivalent to `rsync` without block-level deltas. Incompatible with `--no-clobber`; requires exactly one source directory.
- *(xcp)* New `--hardlinks` flag for `--sync`: detects files that share an inode in the source and recreates the same hard-link structure in the destination. Type conflicts at the destination (symlink or directory where a hard link is expected) are resolved automatically.
- *(xcp)* New `--special` flag: enables copying of special files (character devices, block devices, sockets, FIFOs) during `--sync`. Without this flag, special files are skipped. Also enables block-device copying in normal (non-sync) mode.
- *(xcp)* New `--xattrs` flag for `--sync`: synchronises extended attributes (including POSIX ACLs, stored as xattrs on Linux) even for files that are otherwise unchanged (same mtime, size, permissions). Requires `--sync`.
- *(xcp)* New `--sync-full` flag: activates all sync-related options at once (`--sync --hardlinks --special --xattrs --ownership`); implies `--sync` so it need not be specified separately.

## [0.24.8](https://github.com/tarka/xcp/compare/xcp-v0.24.7...xcp-v0.24.8) - 2026-05-03

### <!-- 1 -->Bug Fixes

- *(xcp)* Update rand to latest and fix API usage.
- *(xcp)* treat source roots that are symlinks the same as deeper symlinks

## [0.24.7](https://github.com/tarka/xcp/compare/xcp-v0.24.6...xcp-v0.24.7) - 2026-02-06

### Other

- Remove warnings->error override as we will get these with cross-platform compilation.
- Add warning about reflinking on Mac.
- Tag already includes `xcp`

## [0.24.6](https://github.com/tarka/xcp/compare/xcp-v0.24.5...xcp-v0.24.6) - 2026-02-06

### Other

- More tweaks to release-plz workflow.
- Convert to using a GH PAT token for release-plz to allow binary releases.

## [0.24.5](https://github.com/tarka/xcp/compare/xcp-v0.24.4...xcp-v0.24.5) - 2026-02-06

### Other

- Fix release binary matcher.

## [0.24.4](https://github.com/tarka/xcp/compare/xcp-v0.24.3...xcp-v0.24.4) - 2026-02-06

### <!-- 4 -->Performance

- Add optimised release profile

### Other

- Add Mac to released binaries.
- Initial release-binaries workflow.
- Add license file
- Update dependencies, including a security issue with `time`

## [0.24.3](https://github.com/tarka/xcp/compare/xcp-v0.24.2...xcp-v0.24.3) - 2026-01-29

Minor maintenance release:

- Releases are now performed with release-plz.
- Add a short AI-contributions policy.

### Other

- Add release-plz config.
- Minor dependency bump.
- Ignore emacs rust-analyser settings.
- Change default branch from 'main' to 'master'
- Include AI Contribution Policy in README
- Bump dependencies.
- Add release-plz workflow.
- Fix root test.
- Update github tests with rust helper actions.
- Add emacs restore files to .gitignore.
- Minor clippy improvement.
- Remove circleci from badges.
- Remove circleci builds as they're not kept up to date currently.
