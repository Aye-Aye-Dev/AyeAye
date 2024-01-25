from collections import namedtuple, defaultdict
from datetime import datetime
from enum import Enum
from time import time
import warnings

import ayeaye
from ayeaye.connectors.base import DataConnector
from ayeaye.connect_resolve import connector_resolver
from ayeaye.runtime.knowledge import RuntimeKnowledge
from ayeaye.runtime.multiprocess import MessageType, LocalProcessPool
from ayeaye.ignition import EngineUrlCase, EngineUrlStatus


class LockingMode(Enum):
    """
    How to capture context around datasets in a model. This is used to track data provenance and
    to make model repeatability possible.

    @see :meth:`Model.lock`
    """

    CONTEXT = "context"  # just the ayeaye.connector_resolver context
    ALL_DATASETS = "all_datasets"  # the engine_urls for all datasets


class Model:
    """
    Do the thing!

    Abstract class

    The thing is probably an ETL (Extract, Transform and Load) task which is at the minimum
    the :meth:`build`. It could optionally also have a :meth:`pre_build_check`, which is run
    first and must succeed. After the :meth:`build` an optional :meth:`post_build_check`
    can be used to see if build worked.
    """

    def __init__(self):
        self._connections = {}  # see :class:`ayeaye.Connect` 'descriptors' in doc string.

        self.log_to_stdout = True
        self.external_logger = None
        self.progress_log_interval = 20  # minimum seconds between progress messages to log

        # stats
        self.start_build_time = None
        self.progress_logged = None  # time last logged
        # subclasses can store counters etc. here. When used, the model will log these when
        # it finishes
        self.stats = defaultdict(int)

    def go(self):
        """
        Run the model.

        The steps to run a model are
        1. :meth:`pre_build_check` - Optional conditional check if model's pre-conditions have been
        met.
        2. :meth:`build` - The main modelling stage
        3. :meth:`post_build_check` - Optional check if the outputs are valid. e.g. a data
        validation check that is simple and concise. It's like a unit test but on changable data.

        Datasets are closed after each stage. This ensures the connections don't have a state, for
        example a position within a file.

        @return: boolean for success

        """
        self.start_build_time = time()
        if not self.pre_build_check():
            self.log("Pre-build check failed", "ERROR")
            self.close_datasets()
            return False
        self.close_datasets()

        self._build()
        self.close_datasets()

        if not self.post_build_check():
            self.log("Post-build check failed", "ERROR")
            self.close_datasets()
            return False
        self.close_datasets()

        if len(self.stats) > 0:
            for stat_field, stat_value in self.stats.items():
                self.log(f"Statistic: {stat_field} = {stat_value}", "INFO")

        return True

    def pre_build_check(self):
        """
        Optionally implemented by subclasses to check any conditions that must be met before
        running :meth:`build`.

        For example, assumptions :meth:`build` makes on on the format or values within the data
        could be checked here in order to keep code in build simple.

        @return: boolean. If False is returned :meth:`build` won't be run.
        """
        return True

    def _build(self):
        """
        Internal stub to allow :class:`Model` and :class:`PartitionedModel` to do different things.
        """
        self.build()

    def build(self):
        """
        Do the thing! - build/process/transform - whatever the model does starts here.

        Must be implemented by subclasses. Don't change method argument in subclass as this is
        called by :meth:`go` when running the full model execution.
        """
        raise NotImplementedError("All models must implement the build() method")

    def post_build_check(self):
        """
        This is an optional method that will be run after :meth:`build`. It can be used to check
        the validity of the build process.

        @return: boolean. If False is returned the model is considered to have failed.
        """
        return True

    @classmethod
    def connects(cls):
        """
        :returns (dict) of :class:`Connect` classes declared as class variables for this model.
                key is class variable name
                value is :class:`ayeaye.Connect`
        """
        # find :class:`ayeaye.Connect` connections to datasets
        connects = {}
        for obj_name in dir(cls):
            obj = getattr(cls, obj_name)
            if isinstance(obj, ayeaye.Connect):
                connects[obj_name] = obj

        return connects

    def datasets(self):
        """
        :returns (dict) of dataset connections for this model.
                key is class variable name
                value is :class:`ayeaye.DataConnector`
        """
        # find :class:`ayeaye.Connect` connections to datasets
        connections = {}
        for obj_name in dir(self):
            obj = getattr(self, obj_name)
            if isinstance(obj, DataConnector):
                connections[obj_name] = obj

        return connections

    def close_datasets(self):
        """
        Call :meth:`close_connection` on all datasets.
        """
        for connection in self.datasets().values():
            if connection.is_connected:
                connection.close_connection()

    def set_logger(self, logger):
        """
        Also log to something with a :meth:`write`.
        e.g. StringIO
        """
        self.external_logger = logger

    def log(self, msg, level="INFO"):
        """
        @param level: (str) DEBUG, PROGRESS, INFO, WARNING, ERROR or CRITICAL
        """
        if not (self.log_to_stdout or self.external_logger is not None):
            return

        date_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        msg = "{} {}{}".format(date_str, level.ljust(10), msg)

        if self.external_logger is not None:
            self.external_logger.write(msg)

        if self.log_to_stdout:
            print(msg)

    def log_progress(self, position_pc, msg=None):
        """
        Externally provided indication of position through build is used to calculate the remaining
        time until this model completes. A `PROGRESS` log message is issued a maximum of every
        `self.progress_log_interval` seconds (default is 20 seconds) so this method can be called
        more frequently and the output log wont be swamped.

        @param position_pc: (float) or None meaning ignore - between 0.0 and 1.0 (complete)
        @param msg: (str) additional user friendly info
        """
        time_now = time()

        if position_pc > 0.0001 and (
            self.progress_logged is None
            or self.progress_logged + self.progress_log_interval < time_now
        ):
            if msg is None:
                msg = ""

            progress_pc = "{:.2f}%".format(position_pc * 100)
            running_secs = time_now - self.start_build_time
            eta = "{:.2f}".format((running_secs / position_pc) - running_secs)
            self.log(f"{progress_pc} {eta} seconds remaining. {msg}", level="PROGRESS")
            self.progress_logged = time_now

    def fetch_locking(self):
        """
        A 'lock' is the information needed by a model to reproduce an output.

        This method is optionally implemented by subclasses that need more than the context
        provided by `ayeaye.connector_resolver` in order to be repeatable. For example, the model
        might not be deterministic.

        The value returned will be passed to :meth:`apply_locking` if/when a model needs to
        be repeated.

        @returns something that can be serialised to JSON
        """
        return None

    def apply_locking(self, lock_doc):
        """
        Use the output from :meth:`fetch_locking` to reproduce results from a previous build.

        This method should be implemented if a model needs to be hydrated with locking details from
        a previous build.
        """
        raise NotImplementedError("Missing sub-class method on attempt to re-hydrate locking.")

    def lock(self, locking_level=LockingMode.CONTEXT):
        """
        A 'lock' is the information needed by a model to reproduce an output.

        Return a JSON safe dictionary of variables needed to repeat the build.

        The returned dictionary is expected to be serialised and stored.

        @param locking_level: (LockingMode)
            LockingMode.CONTEXT - key values from ayeaye.connector_resolver
            LockingMode.ALL_DATASETS - CONTEXT + engine_urls from all datasets in model.

        @return (dict)
        """
        # this is very much work in progress.
        # coming soon
        # - code version (git commitishes)
        # - library code versions (maybe pipenv lock file)

        locking_doc = {"resolve_context": connector_resolver.capture_context()}

        model_lock = self.fetch_locking()
        if model_lock is not None:
            locking_doc["model_locking"] = model_lock

        if locking_level == LockingMode.ALL_DATASETS:
            locking_doc["dataset_engine_urls"] = {}
            for dataset_name, connector in self.datasets().items():
                # TODO: this should be EngineUrlCase.WITHOUT_SECRETS. Secrets shouldn't be in
                # locking doc. But that's not implemented yet,
                status, engine_url = connector.ignition.engine_url_at_state(
                    EngineUrlCase.FULLY_RESOLVED
                )
                msg = (
                    "Incomplete implementation: if there are secrets in the engine_url they "
                    "will be included in locking doc."
                )
                warnings.warn(msg)

                if status != EngineUrlStatus.OK:
                    raise ValueError(f"Can't lock, engine_url not available for '{dataset_name}'")

                locking_doc["dataset_engine_urls"][dataset_name] = engine_url

        return locking_doc


class PartitionedModel(Model):
    """
    Similar to :class:`Model` but requires additional methods to be implemented by the subclass to
    describe how the processing can be split into parallel subtasks.

    A model can suggest to the executor how many parallel tasks could be used and the executor
    is responsible for orchestrating a level of parallelisation that fits the execution environment.

    An external executor is expected to run :class:`PartitionedModel` subclasses. It would
    instantiate the model and examine it's :meth:`partition_plea` before running it in a
    distributed environment (e.g. Google Cloud Run, Celery, AWS' ECS etc.). The external executor
    is responsible for transporting task arguments, log message etc. TODO : there isn't an open
    source executor yet. It has been done in a commercial project.

    :class:`PartitionedModel` does support a simple mechanism for parallel execution across local
    processes. To use it implement the mandatory methods and run method:`go`.

    e.g.
    >>> my_model = MyModel()
    >>> my_model.go()

    And a reasonable number of local processes will run in a Python multiprocessor Pool to work
    on the tasks the model divides itself into.

    The executor is responsible for re-executing failed tasks. All sub-tasks should be idempotent and
    therefore safe to execute multiple times, possibly in parallel.

    It works as follows-

    1. The executor creates the parent instance. If the 'simple mechanism for parallel execution' (as
    above) is being used, this is the instance that :meth:`go` is running on.

    2. The executor asks the parent instance for 'suggestions' of how many sub-tasks the model can
    be split into (see :meth:`partition_plea`). This suggestion is for the number of workers the
    model would like to have running in parallel. There are situations where an exact number of
    workers are needed (e.g. so each worker has independent control of a resource such as a file)
    and there are other situations where too many worker could result in a stampede on a resource
    (such as a database).

    3. The executor evaluates the :class:`PartitionOption` returned by :meth:`partition_plea`
    along with the capabilities of the execution environment and requests a mutually compatible
    number of partition arguments from :meth:`partition_slice`. The number of sub-tasks doesn't
    need to match the number of workers although there can be a relationship.

    4. :meth:`partition_slice` is called on the parent instance. It returns a list of method
    names and sub-task arguments. Each of which is passed by the executor to an instance of the
    model that has been instantiated with the same resolver context (see :class:`ConnectorResolver`)
    as the 'parent' model. The models running in the worker processes are initiated when the worker
    process starts, i.e. each instance will have it's sub-task method called multiple times. See
    :meth:`partition_initialise`.

    4. The return value from each subtask is passed to optional :meth:`partition_subtask_complete`.

    5. When the executor is satisfied that all sub-tasks are complete the optional
    :meth:`partition_complete` method is called.

    6. The :meth:`build` will be called in a separate worker process. i.e. this isn't the same
    instance as the parent instance.
    """

    # Start simple, this will no doubt increase in flexibility. Each are an integer suggesting
    # how many sub-tasks the execution could be split into.
    PartitionOption = namedtuple("PartitionOption", ("minimum", "maximum", "optimal"))

    def __init__(self):
        super().__init__()

        # the link between the execution environment and the process
        self.runtime = RuntimeKnowledge()

        # lazy / injectable
        self._process_pool = None

    @property
    def process_pool(self):
        """
        Set the way subtasks are processed in parallel. Do this before using the model.

        The It's used to change how subtasks are run. The default is to use :class:`LocalProcessPool`
        which uses multiple :class:`multiprocessing.Process`es but a distributed pool could be used
        instead.
        see Fossa repo :class:`fossa.control.rabbit_mq.message_exchange` for another
        example.

        @return: subclass of :class:`ayeaye.runtime.multiprocess.AbstractProcessPool`
        """
        if self._process_pool is None:
            # default behaviour is to run subtasks in separate local processes
            self._process_pool = LocalProcessPool(max_processes=self.runtime.max_concurrent_tasks)

        return self._process_pool

    @process_pool.setter
    def process_pool(self, alternative_process_pool):
        """
        Override the default process pool. Maybe with a distributed one.
        @param alternative_process_pool: object as subclass of
            :class:`ayeaye.runtime.multiprocess.AbstractProcessPool`
        """
        # Note -no check for existing pool. A user could trash an existing running set of subtasks
        self._process_pool = alternative_process_pool

    def partition_initialise(self, *args, **kwargs):
        """
        This method will be called when a worker process instantiates a model. This method can
        be overridden but sub-class's method must be called.
        """
        self.start_build_time = time()

        return None

    def partition_plea(self):
        """
        Subclass to suggest possible options for splitting the task execution.

        This is the mechanism for the model to explain to the execution environment how the task
        could be split into sub-tasks.

        The default behaviour is to return `PartitionOption(minimum=1, maximum=128, optimal=16)`.

        @return (PartitionOption)
        """
        return PartitionedModel.PartitionOption(minimum=1, maximum=128, optimal=16)

    def partition_slice(self, partition_count):
        """
        Create sub-task arguments.

        Create a list (sorry, no iterators) of arguments to define each subtask. This is composed
        of the method name and key word arguments. The method+kwargs will be called on one of the
        workers.

        The number of sub-tasks returned doesn't need to relate to the number of partitions
        (`partition_count`) that will be used as each worker could execute zero or more than one
        sub-task. `partition_count` is passed to :meth:`partition_slice` as there are scenarios
        where a greater efficiency is possible when the number of sub-tasks is a multiple of the
        number of workers.

        @param partition_count: (int)
            The number of workers the executor plans to run.

        @returns (list of (method name (str), kwargs [i.e. a dict])
        """
        raise NotImplementedError("All models must implement this method")

    def partition_subtask_complete(self, subtask_method_name, subtask_kwargs, subtask_return_value):
        """
        Optional method. Takes return values from subtasks.

        This will be called on the parent task (i.e. not on the worker). It can be used to collate
        results or take further actions when a sub-task has finished.
        """
        return None

    def partition_complete(self):
        """
        Optional method. Called when executor has finished all sub-tasks.
        """
        return None

    def worker_initialise(self, processes):
        """
        Optional method. This is called by the executor after the number of workers has been
        determined if the number of workers is a fixed number.

        @param processes: (int)
            The number of workers the executor plans to run.

        @return: list of tuples containing args (list) or kwargs (dict) or None when not needed.
            Each item in this list is used to initialise a single worker. It could contain
            a worker ID. The number of items must equal the number of partitions. An exception
            will be raised if this isn't the case.
        """
        return None

    def _build(self):
        """
        When not run by an executor in a distributed environment the default behaviour is for
        :class:`PartitionedModel`s to behave like normal :class:`Model`s but using
        multiprocessing for local parallel execution.

        This 'built in' concurrent operator sets up a pool of processes that is related to the
        number of sub-tasks indicated by :meth:`partition_plea`. There doesn't need to be
        a relationship between the number of processes and the number of tasks but it wouldn't
        make sense for more processes than tasks.
        """

        partition_option = self.partition_plea()

        msg = "Partition minimum must be > 0 and < maximum"
        assert partition_option.minimum > 0, msg
        assert partition_option.minimum <= partition_option.maximum, msg

        # this should be in lock-step with the default LocalProcessPool and other pools do their
        # own thing.
        max_workers = self.runtime.max_concurrent_tasks

        workers_count = partition_option.minimum
        if partition_option.optimal < max_workers:
            workers_count = partition_option.optimal
        else:
            workers_count = max_workers

        if workers_count > partition_option.maximum:
            workers_count = partition_option.maximum

        # workers_count =  1
        self.log(f"Using {workers_count} worker processes")

        self.build()

        tasks = self.partition_slice(workers_count)
        subtasks_count = len(tasks)

        # model can specify arguments for initialising workers
        worker_init = self.worker_initialise(processes=workers_count)

        active_context = connector_resolver.capture_context()

        if workers_count == 1:
            # don't use the process pool as only one worker is available. There might be many
            # tasks so do these in serial.
            # this mode is useful for unittests as Multiprocess can confuse things
            self.log(f"Running single sub-task within main process")

            # simplified verion of what happens in ProcessPool.
            # The resolver context and class are currently assumed to be the same as `self` has
            # so :meth:`partition_initialise` isn't called.

            self.runtime.worker_id = 0
            self.runtime.total_workers = 1

            subtasks_complete = 0
            for method_name, method_kwargs in tasks:
                if method_kwargs is None:
                    method_kwargs = {}

                sub_task_method = getattr(self, method_name)
                subtask_return_value = sub_task_method(**method_kwargs)
                subtasks_complete += 1

                self.partition_subtask_complete(
                    subtask_method_name=method_name,
                    subtask_kwargs=method_kwargs,
                    subtask_return_value=subtask_return_value,
                )
                self.log_progress(subtasks_complete / subtasks_count)

        else:
            subtasks_complete = 0
            subtask_kwargs = {
                "model_cls": self.__class__,
                "sub_tasks": tasks,
                "initialise": worker_init,
                "context_kwargs": active_context,
                "processes": workers_count,
            }
            for subtask_return in self.process_pool.run_subtasks(**subtask_kwargs):
                message_type = subtask_return[0]

                if message_type == MessageType.COMPLETE:
                    (
                        message_type,
                        method_name,
                        original_method_kwargs,
                        subtask_return_value,
                    ) = subtask_return
                    self.partition_subtask_complete(
                        subtask_method_name=method_name,
                        subtask_kwargs=original_method_kwargs,
                        subtask_return_value=subtask_return_value,
                    )
                    subtasks_complete += 1
                    self.log_progress(subtasks_complete / subtasks_count)

                elif message_type == MessageType.LOG:
                    # TODO structured logging to separate and de-dupe fields like the date
                    self.log(subtask_return[1])
                else:
                    raise ValueError("Undefined message type received")

        self.partition_complete()
