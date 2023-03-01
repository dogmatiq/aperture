<div align="center">

# Aperture

Aperture is an intensely minimal **projection-only**
[Dogma](https://github.com/dogmatiq/dogma)
[engine](https://github.com/dogmatiq/dogma#engine).

[![Documentation](https://img.shields.io/badge/go.dev-documentation-007d9c?&style=for-the-badge)](https://pkg.go.dev/github.com/dogmatiq/aperture)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/aperture.svg?&style=for-the-badge&label=semver)](https://github.com/dogmatiq/aperture/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/dogmatiq/aperture/ci.yml?style=for-the-badge&branch=main)](https://github.com/dogmatiq/aperture/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/aperture/main.svg?style=for-the-badge)](https://codecov.io/github/dogmatiq/aperture)

</div>

Aperture's primary purpose is to integrate Dogma
[projections](https://github.com/dogmatiq/dogma#projection) with event-driven
systems that are not necessarily already using Dogma. Given a user-supplied
source of event [messages](https://github.com/dogmatiq/dogma#message), it
provides the bare minimum of logic needed to correctly apply those events to
Dogma projections.
