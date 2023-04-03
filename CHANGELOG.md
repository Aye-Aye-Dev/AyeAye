# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- nothing

### Changes
- PartitionedModel now runs .build() before subtasks. This is a change in behaviour to ensure .build() runs before subtasks. The execution of subtasks isn't ordered.

## [0.0.36] - 2023-03-28

### Fixed
- Optional 'requests' library was non-optional import

## [0.0.35] - 2023-01-07

### Added
- 'with' statement usage of resolver context to allow variables to be added after initialisation

### Removed
- FlowerPotConnector, GcsFlowerpotConnector, FlowerpotEngine - they havn't been supported for some time and they don't look very useful 

## [0.0.34] - 2022-12-16

### Added
- Easier access to SqlAlchemy query methods with .query on :class:`SqlAlchemyDatabaseConnector`

### Changed
- check to ensure engine_urls are fully resolved before using a pattern match

## [0.0.33] - 2022-12-12

### Added
- Use of wildcards in engine_url param of :class:`ayeaye.Connect` to be pattern matched after the context variables are resolved. This means `engine_url="csv://{my_data}/*.csv"` will result in a MultiConnector after 'my_data' has been resolved into a runtime specified path.
- Connectors which use the filesystem will all have the wildcard behaviour
- :class:`ayeaye.Model` has `self.stats` as a default dictionary. It's aimed at runtime statistics ga
thering. When used it automatically logs the results after the model has finished running

## [0.0.32] - 2022-11-25

### Added
- Silently ignore fields not in `field_names` list when writing CSV data with :method:`add`

### Fixed
- auto create CSV directory when using a relative path

## [0.0.31] - 2022-10-13

### Added
- 'method_overlay' to ayeaye.Connect/DataConnector so it's possible to dynamically add methods to a data connector without needing inheritance.

### Fixed
- RestfulConnector exception when returned doc. isn't JSON

## [0.0.30] - 2022-07-25

### Fixed
- Pinnate top level not correctly initialised when __contains__ is first accessor method

## [0.0.29] - 2022-07-25

### Fixed
- Pinnate top level not correctly initialised when __setitem__ is first accessor method

## [0.0.28] - 2022-07-19

### Removed
- Pinnate.merge - it wasn't doing anything that .update does

### Added
- Pinnate now supports top level lists and sets. Thanks @burnleyrob

## [0.0.27] - 2022-07-14

### Added
- New RestfulConnector for access JSON RestFul APIs. Code kindly contributed by Bluefield Services
- Added package extra ayeaye[api] to include requests library
- ModelCollection is top level public class
- ModelCollection can now build data provenance graphs for simple model collections - i.e. those that
 don't need any resolver context or locking info
- ModelCollection is incomplete and has a failing test

## [0.0.26] - 2022-06-09

### Fixed
- multiprocess support for ayeaye.PartitionedModel had different behaviour on OSX and Linux because of parent memory after fork() with resolver context

## [0.0.25] - 2022-06-08

### Added
- manifest_build_context() common pattern - make values from a manifest file available in the resolver context
- example usage for ayeaye.common_pattern.build_context.manifest_build_context

### Changed
- ayeaye.connect_resolve supports lists, dict and anything else JSON serialisable. Previously just simple variables.

## [0.0.24] - 2022-05-09

### Removed
- ModelsConnector - it makes better sense outside the ayeaye.Connect(..) declaration

### Added
- ModelCollection - the old ModelsConnector slightly repackaged
- ayeaye.common_pattern.build_context.manifest_build_context and examples/manifest_build_context to demonstrate a simple way to list files used in an ayeaye.Model

## [0.0.23] - 2022-03-10

### Added
- ayeaye.Connect now supports filesystem wildcards

## [0.0.22] - 2022-02-11

### Added
- ProcessPool workers run within the connector_resolver context. Just key+value pairs supported.

## [0.0.21] - 2022-01-31

### Changed
- 'executors' now called 'runtime'

### Added
- A clearer link between runtime environment and PartitionedModel
- worker_id and number of workers (when using fixed sized pool) to runtime info

## [0.0.20] - 2022-01-30

### Added
- ProcessPool to allow init arguments to be passed to models within individual worker processes

## [0.0.19] - 2022-01-30

### Fixed
- Missing executors package

## [0.0.18] - 2022-01-30

### Fixed
- Missing executors package

## [0.0.17] - 2022-01-29

### Added
- PartitionedModel - supports multiprocess execution of subtasks.

## [0.0.16] - 2022-01-26
### Changed
- UncookedConnector - Encoding defaults to None.

### Added
- UncookedConnector - supports file_mode and encoding.

## [0.0.15] - 2021-12-02
### Fixed
- SqlAlchemyDatabaseConnector needs to call it's own callable optional args so it can supply the declarive base argument. i.e. non-simple callable. Created a mechanism for Connectors to reserve this right so Connect() will leave these callables.

## [0.0.14] - 2021-12-01
### Changed
- .schema property has been removed from :class:`DataConnector` - it should whatever type of object the sub class needs

## [0.0.13] - 2021-11-29
### Changed
- any kwarg given to ayeaye.Connect can be a callable, not just engine_url. This isn't quite there for stand-alone connectors but fine when used within `ayeaye.Model`.

## [0.0.12] - 2021-11-29
### Added
- AbstractManifestMapper, EngineFromManifest to public interface

## [0.0.11] - 2021-11-28
### Added
- AbstractManifestMapper more flexible at supporting methods in subclasses by making manifest data available without specifying a fieldname or needing to access via an iterable.

### Fixed
- unitests are skipped when modules aren't available

## [0.0.10] - 2021-10-15
### Added
- CsvConnector - added optional arguments: required_fields, expected_fields, alias_fields

## [0.0.9] - 2021-06-30
### Added
- PartitionedModel - starter for models splitting the work into subtasks

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
