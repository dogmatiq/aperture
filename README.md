# Aperture

[![Build Status](https://github.com/dogmatiq/aperture/workflows/CI/badge.svg)](https://github.com/dogmatiq/aperture/actions?workflow=CI)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/aperture/master.svg)](https://codecov.io/github/dogmatiq/aperture)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/aperture.svg?label=semver)](https://semver.org)
[![Documentation](https://img.shields.io/badge/go.dev-reference-007d9c)](https://pkg.go.dev/github.com/dogmatiq/aperture)
[![Go Report Card](https://goreportcard.com/badge/github.com/dogmatiq/aperture)](https://goreportcard.com/report/github.com/dogmatiq/aperture)

Aperture is an intensely minimal **projection-only**
[Dogma](https://github.com/dogmatiq/dogma)
[engine](https://github.com/dogmatiq/dogma#engine).

It's primary purpose is to integrate Dogma
[projections](https://github.com/dogmatiq/dogma#projection) with event-driven
systems that are not necessarily already using Dogma. Given a user-supplied
source of event [messages](https://github.com/dogmatiq/dogma#message), it
provides the bare minimum of logic needed to correctly apply those events to
Dogma projections.
