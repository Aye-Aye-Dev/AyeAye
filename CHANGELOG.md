# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
 
### Added
- nothing

## [0.0.66] - 2024-07-26

### Added
- :meth:`ConnectorResolver.add_secret` to separate variables that shouldn't be included in locking
- Values given to ConnectorResolver's kwargs (:meth:`add`) can be callables.

### Updated
- ConnectorResolver named attributes couldn't ever be integers as they are passed as kwargs

## [0.0.65] - 2024-07-26

### Updated
- Tiny change to the message when an exception occurs. Now includes type errors.

## [0.0.64] - 2024-07-25

### Added
- Pinnate now supports serialise/pickle

## [0.0.63] - 2024-07-18

### Fixed
- mistake in doc. strings
- local single worker mode for partitioned models wasn't closing datasets at end of sub-task

## [0.0.62] - 2024-07-09

### Added
- check for existing process pool when loading one

### Changed
- copy in exception
- doc strings
- single process mode with PartitionedModels uses same logging as parent task
- RuntimeKnowledge's cpu_task_ratio can be a float


## [0.0.61] - 2024-04-24

### Changed
- Model's use of external logger to instead support multiple external loggers in the same as Fossa does

## [0.0.60] - 2024-04-03

### Fixed
- hung if PartitionedModel.partition_slice returns an empty list
- stray bracket resulting in a couple of unittests being skipped

## [0.0.59] - 2024-04-03

### Added
- optional task_id and failure_origin_task_id fields to ayeaye.runtime.task_message.TaskFailed. Theseare useful to Fossa

## [0.0.58] - 2024-03-28

### Added
- A PartitionedModel can run other PartitionedModel(s)

### Changed
- LocalProcessPool.run_subtasks no longer creates daemon Processes. This makes it possible to a Par
tionedTask to spawn other Partitioned tasks. The downside is the potential for orphaned subtasks.

### Fixed
- AbstractProcessPool.run_subtasks's context_kwargs should be a plain dictionary and doesn't need the 'mapper' key from :meth:`ayeaye.ayeaye.connect_resolve.ConnectorResolver.capture_context`

## [0.0.57] - 2024-03-27

### Changed
- SubTaskFailed exception to include traceback info
- TaskPartition to require the class. This makes it easier for one model to run another. The simple version of `partition_slice` defaults to the current class.
- AbstractProcessPool.run_subtasks to no longer take the model class as this is now in TaskPartition updated: no longer serialising TaskPartition to json. Instead allowing python's queue to do this in LocalProcessPool.


## [0.0.56] - 2024-03-25

### Changed
- PartitionedModel.partition_initialise to only take key word args, not list of args
- PartitionedModel.partition_plea to be able to return list of TaskPartition objects which allow for richer initialisation of the worker
- (breaking change) - interface from AbstractProcessPool.run_subtasks to take TaskPartition objects instead of tuples.
- single process execution in PartitionedModel is now more like multiprocess execution as the model is constructed and partition is initialised for each sub-task.

### Removed
- PartitionedModel.worker_initialise - it doesn't do anything that can't be achieved with PartitionedModel.partition_initialise plus .partition_slice

## [0.0.55] - 2024-02-09

### Added
- the DELETE http verb via .delete in the restful connector - thanks RA!

## [0.0.54] - 2024-01-31

### Added
- a custom exception for when a subtask fails
- optional PartitionedModel.partition_subtask_failed hook to allow models to handle subtask exceptions

### Changed
- default behaviour on subtask fail in a PartitionedModel to raise the SubTaskFailed exception
- LocalProcessPool to cleanup orphan Processes on deconstruction
- TaskFailed to include a couple of extra fields - model_class_name and resolver_context

## [0.0.53] - 2024-01-30

### Fixed
- serialised task messages were being yielded by `LocalProcessPool`

## [0.0.52] - 2024-01-29

### Added
- richer classes (ayeaye.runtime.task_message) for task message within processing pool

### Changed
- yield type from `AbstractProcessPool.run_subtasks`

### Removed
- ayeaye.runtime.multiprocess.MessageType

## [0.0.51] - 2024-01-25

### Added
- AbstractProcessPool to make it easier to add custom process pools - see the Fossa project

### Changed
- PartitionedModel to have injectable :class:`AbstractProcessPool`
- ProcessPool - moved arguments from constructor to run_tasks to make it easier to customise subclasses of ProcessPool
- renamed: ProcessPool to LocalProcessPool to clarify the difference with the new pool being created in the Fossa project

### Removed
- model_initialise option in ProcessPool as ProcessPool is only used by PartitionedModel which doesn't use it and it's simplier without.

## [0.0.50] - 2023-12-12

### Added
- common pattern to make it easy to add datasets to a MultiConnector
- `method_overlay` argument to Connector to allow explicit naming of the new method which makes it
cleaner and more intuative with callables (instead of plain functions)

## [0.0.49] - 2023-12-04

### Added
- Two optional args to CsvConnector. 'quoting' - to pass a mode through to the CSV module and 'transform_map' so a callable can be used at the field level. This is a useful mechanism to make small field level adjustments (e.g. tranforming types) during IO.

### Fixed
- RestfulConnector using wrong fieldname within an exception

## [0.0.48] - 2023-11-02

### Added
- PartitionedModel separates CPU resource into .runtime and no longer runs multiple processes when
the maximum number of parallel workers is 1. This will make unittests easier to manage.

## [0.0.47] - 2023-07-11

### Changed
- The multiprocessing.Queue connecting sub-task processes in PartitionedModel to the parent to be a multiplexed queue with message types. This makes it simple to connect the logs from sub-tasks to the parent's logger. This is an internal change that opens up where log messages surface.

### Added
- A common patten module to show how multiple independent models can be run in parallel using a PartitionedModel

## [0.0.46] - 2023-06-27

### Changed
- MultiConnector.add_engine_url to de-duplicate engine_urls and return a previously built connector when a duplicate engine_url is passed.

## [0.0.45] - 2023-06-15

### Added
- overlay methods are now possible with MultiConnector and 'child_method_overlay' can be used to pass a callable to child connectors (of the parent multi connector).

## [0.0.44] - 2023-06-07

### Fixed
- SqlAlchemyDatabaseConnector's .add() couldn't add ORM instance when in single model mode

## [0.0.43] - 2023-06-05

### Added
- .last_modified to FileBasedConnector connectors. Thanks burnleyrob!

### Changed
- CsvConnector, ParquetConnector, NdjsonConnector,  closes the file handle when iteration has finished with the file

### Fixed
- iterating within an iterator for a cloned data connector resulted in premature EOF

## [0.0.42] - 2023-04-25

### Changed
- unknown engine_type now throws ayeaye.exception.UnknownEngineType instead of NotImplementedError

### Fixed
- it was possible to read from a JsonConnector when in AccessMode.WRITE mode

## [0.0.41] - 2023-04-25

### Changed
- subclasses of DataConnector (i.e. the connectors) *must* now call the super class's connect() and
.close_connection() methods. This is a silent breaking change. Not implementing this could result in resour
ces not being correctly freed when a connection is closed.

## [0.0.40] - 2023-04-21

### Changed
- additional checks to the process of registering a connector

### Fixed
- MultiConnector children not having a registed `Connect` instance

## [0.0.39] - 2023-04-13

### Added
- Smart open is the first engine_type modifier. It now supports compression with all DataConnectors that subclass `FileBasedConnector`. So engine_urls like 'gz+ndjson://myfile.ndjson.gz' are now supported.
- s3 engine type modifer
- an example ayeaye.Model that uses s3+gz+csv
- S3 engine types can use wildcards to match multiple files into a MultiConnector. See NoaaClimatology in examples.

## [0.0.38] - 2023-04-11

### Added
- The RestfulConnector can be passed raw data to pass directly to the request.post method

## [0.0.37] - 2023-04-11

### Added
- engine_type (i.e. engine_type://xxx) can be context resolved.

### Changed
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
