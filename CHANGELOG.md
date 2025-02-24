# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)

## [Unreleased]

### Added
- Added fields to manage expiration for certificates.

### Fixed
- PutValue bugs (fails to override old (expired) record, fails to put on multiple peers).

### Changed
- Use standard protobuf (google.golang.org/protobuf) instead of gogo protobuf (deprecated).
  (Requires boxo latest version)

### Removed


