# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## v4.1.2 - 2024-09-26

### Added

- [PIPELINE-1871](https://globalfishingwatch.atlassian.net/browse/PIPELINE-1229):
  Removed logistic_score and regions field to meet pipe-features v4 output

## v4.1.1 - 2024-04-29

### Added

- [PIPELINE-1871](https://globalfishingwatch.atlassian.net/browse/PIPELINE-1871): Fix
  arguments to be backwards compatible

## v4.1.0 - 2024-04-16

### Added

- [PIPELINE-1871](https://globalfishingwatch.atlassian.net/browse/PIPELINE-1871): Use
  new research query to generate vms research positions.

## v3.0.2 - 2021-11-19

### Added

- [PIPELINE-626](https://globalfishingwatch.atlassian.net/browse/PIPELINE-626): Adds
  the lowercase of the `source` property in the output of the research positions.

## v3.0.1 - 2021-11-11

### Added

- [PIPELINE-617](https://globalfishingwatch.atlassian.net/browse/PIPELINE-617):
  Query that checks if there are fishing hours under a particular threshold per day.

## v3.0.0 - 2021-10-25

### Added

- [PIPELINE-572](https://globalfishingwatch.atlassian.net/browse/PIPELINE-572):
  Created project for VMS summary table calculation.
  Upgraded the VMS summarized positions query to run in `yearly` and `monthly` mode.
