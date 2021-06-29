# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.0.8] - 2021-06-29
### Fixed
- updated layout to better fit an easy life with packaging and distribution.

## [0.0.7] - 2021-06-29
### Fixed
- Moved from build-0.4.0 to build-0.5.1 as sub packages were appearing at top level
- removed lazy methods in AbstractManifestMapper. They are too complicated to get right when the subclass is used as a class variable that depends on another descriptor.

### Added
- AbstractManifestMapper is now a descriptor

### Changed
- AbstractManifestMapper iterator uses single engine_url instead of list of 1 item. Hopefully this is more intuative.

## [0.0.6] - 2021-06-23
### Added
- AbstractManifestMapper and an example demonstrating a useful pattern for mapping between files listed in a mani
fest

### Fixed
- common_pattern.manifest.EngineFromManifest was loading the manifest too early
- Connect.clone and Connect.copy weren't taking arguments passed to .update()

## [0.0.5] - 2021-06-22
### Fixed
- missing common patterns package

## [0.0.4] - 2021-06-22
### Added
- dictionary access via engine_url in MultiConnector

## [0.0.3] - 2021-06-18
### Added
- alternative locking mode (LockingMode.ALL_DATASETS) to capture engine_urls from all datasets in a model
- common pattern to use a manifest file - 'ayeaye.common_pattern.manifest.EngineFromManifest'

## [0.0.2] - 2021-06-16
### Added
- ayeaye.Model.lock() - starting point for model locking 

### Deprecated
- ConnectorResolver.resolve_engine_url renamed to resolve
