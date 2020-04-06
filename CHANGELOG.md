# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->
[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [0.3.2] - 2020-04-06

### Fixed

- Prevent memory cursor from blocking after filtering events on a sealed stream

## [0.3.1] - 2020-03-01

### Added

- Add `ordered/resource` package for low-level OCC resource manipulation

## [0.3.0] - 2020-01-30

### Changed

- **[BC]** Update to `dogmatiq/configkit` v0.3.0

## [0.2.2] - 2020-01-23

### Fixed

- Fix collision between `dogma.handler.type` and `dogma.message.role` tracing attributes

## [0.2.1] - 2020-01-16

### Added

- Add `ordered.ErrStreamSealed`
- Add `MemoryStream.Seal()`

### Changed

- `Stream.Open()` and `Cursor.Next()` may now return `ErrStreamSealed`
- `MemoryStream.ID()` now panics if the `StreamID` field is empty
- `MemoryStream.Append()` now panics if `Seal()` has been called
- `MemoryStream.Append()` now panics if any of the given messages is `nil`

### Fixed

- Fix unconditional OCC failure in `ordered.Projector`

## [0.2.0] - 2020-01-14

### Added

- Add `MemoryStream.Truncate()`
- Add metrics and tracing support to `Projector` via OpenTelemetry

### Changed

- Use configkit instead of enginekit

## [0.1.0] - 2019-11-07

- Initial release

<!-- references -->
[0.1.0]: https://github.com/dogmatiq/aperture/releases/tag/v0.1.0
[0.2.0]: https://github.com/dogmatiq/aperture/releases/tag/v0.2.0
[0.2.1]: https://github.com/dogmatiq/aperture/releases/tag/v0.2.1
[0.2.2]: https://github.com/dogmatiq/aperture/releases/tag/v0.2.2
[0.3.0]: https://github.com/dogmatiq/aperture/releases/tag/v0.3.0
[0.3.1]: https://github.com/dogmatiq/aperture/releases/tag/v0.3.1

[Unreleased]: https://github.com/dogmatiq/aperture

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
