from collections import namedtuple
from datetime import datetime
from enum import Enum
from time import time
import warnings

import ayeaye
from ayeaye.connectors.base import DataConnector
from ayeaye.connect_resolve import connector_resolver
from ayeaye.ignition import EngineUrlCase, EngineUrlStatus


class LockingMode(Enum):
    """
    How to capture context around datasets in a model. This is used to track data provenance and
    to make model repeatability possible.

    @see :method:`Model.lock`
    """
    CONTEXT = 'context'  # just the ayeaye.connector_resolver context
    ALL_DATASETS = 'all_datasets'  # the engine_urls for all datasets


class Model:
    """
    Do the thing!

    Abstract class 

    The thing is probably an ETL (Extract, Transform and Load) task which is at the minimum
    the :method:`build`. It could optionally also have a :method:`pre_build_check`, which is run
    first and must succeed. After the :method:`build` an optional :method:`post_build_check`
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

    def go(self):
        """
        Run the model.

        The steps to run a model are
        1. :method:`pre_build_check` - Optional conditional check if model's pre-conditions have
           been met.
        2. :method:`build` - The main modelling stage
        3. :method:`post_build_check` - Optional check if the outputs are valid. e.g. a data
           validation check that is simple and concise. It's like a unit test but on changable
           data.

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

        self.build()
        self.close_datasets()

        if not self.post_build_check():
            self.log("Post-build check failed", "ERROR")
            self.close_datasets()
            return False
        self.close_datasets()

        return True

    def pre_build_check(self):
        """
        Optionally implemented by subclasses to check any conditions that must be met before
        running :method:`build`.

        For example, assumptions :method:`build` makes on on the format or values within the data
        could be checked here in order to keep code in build simple.

        @return: boolean. If False is returned :method:`build` won't be run.
        """
        return True

    def build(self):
        """
        Do the thing! - build/process/transform - whatever the model does starts here.

        Must be implemented by subclasses. Don't change method argument in subclass as this is
        called by :method:`go` when running the full model execution.
        """
        raise NotImplementedError("All models must implement the build() method")

    def post_build_check(self):
        """
        This is an optional method that will be run after :method:`build`. It can be used to check
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
        Call :method:`close_connection` on all datasets.
        """
        for connection in self.datasets().values():
            connection.close_connection()

    def set_logger(self, logger):
        """
        Also log to something with a :method:`write`.
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
        Externally provided indication of position through build is used to calculate ETA (TODO)
        and occasionally log progress to show user something is happening.

        @param position_pc: (float) or None meaning ignore
        @param msg: (str) additional user friendly info
        """
        time_now = time()

        if position_pc > 0.0001 and \
            (self.progress_logged is None or
             self.progress_logged + self.progress_log_interval < time_now):

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

        The value returned will be passed to :method:`apply_locking` if/when a model needs to
        be repeated.

        @returns something that can be serialised to JSON
        """
        return None

    def apply_locking(self, lock_doc):
        """
        Use the output from :method:`fetch_locking` to reproduce results from a previous build.

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

        locking_doc = {'resolve_context': connector_resolver.capture_context()
                       }

        model_lock = self.fetch_locking()
        if model_lock is not None:
            locking_doc['model_locking'] = model_lock

        if locking_level == LockingMode.ALL_DATASETS:
            locking_doc['dataset_engine_urls'] = {}
            for dataset_name, connector in self.datasets().items():

                # TODO: this should be EngineUrlCase.WITHOUT_SECRETS. Secrets shouldn't be in
                # locking doc. But that's not implemented yet,
                status, engine_url = connector.ignition.engine_url_at_state(
                    EngineUrlCase.FULLY_RESOLVED)
                msg = ("Incomplete implementation: if there are secrets in the engine_url they "
                       "will be included in locking doc."
                       )
                warnings.warn(msg)

                if status != EngineUrlStatus.OK:
                    raise ValueError(f"Can't lock, engine_url not available for '{dataset_name}'")

                locking_doc['dataset_engine_urls'][dataset_name] = engine_url

        return locking_doc


class PartitionedModel(Model):
    """
    Similar to :class:`Model` but requires additional methods to be implemented by the subclass to
    describe how the processing can be split into parallel subtasks.

    A model can suggest to the executor how many parallel tasks could be used and the executor
    is responsible for orchestrating a level of parallelisation that fits the execution environment.

    The :method:`PartitionedModel.build` can run a model across multiple local processes or an
    external executor (e.g. Google Cloud Run, Celery, AWS' ECS etc.) could run the tasks across a
    distributed environment.

    The executor is responsible for re-executing failed tasks. All sub-tasks should be idempotent and
    therefore safe to execute multiple times, possibly in parallel.

    It works as follows-

    1. The executor asks the model for 'suggestions' of how many sub-tasks the model can be split
    into (see :method:`partition_plea`).

    2. The executor evaluates the :class:`PartitionOption` returned by :method:`partition_plea`
    along with the capabilities of the execution environment and requests a mutually compatible
    number of partition arguments from :method:`partition_slice`.

    3. :method:`partition_slice` returns a list of method names and arguments. Each of which is
    passed by the executor to an instance of the model that has been instantiated with the same
    resolver context (see :class:`ConnectorResolver`) as the 'parent' model.

    4. The return value from each subtask is passed to optional :method:`partition_subtask_complete`.

    5. When the executor is satisfied that all sub-tasks are complete the optional
    :method:`partition_complete` method is called.
    """

    # Start simple, this will no doubt increase in flexibility. Each are an integer suggesting
    # how many sub-tasks the execution could be split into.
    PartitionOption = namedtuple('PartitionOption', ('minimum', 'maximum', 'optimal'))

    def partition_plea(self):
        """
        Subclass to suggest possible options for splitting the task execution.

        @return (PartitionOption)
        """
        raise NotImplementedError("All models must implement this method")

    def partition_slice(self, slice_count):
        """
        Not an iterator - return a list so the sub-class model is simple in terms of concurrency
        and is ready to accept calls to :method:`partition_subtask_complete`.

        @returns (list of (method name (str), kwargs [i.e. a dict])
        """
        raise NotImplementedError("All models must implement this method")

    def partition_subtask_complete(self, subtask_kwargs, subtask_return_value):
        """
        Optional method. Takes return values from subtasks.
        """
        return None

    def partition_complete(self):
        """
        Optional method. called when executor has finished all sub-tasks.
        """
        return None

    def build(self):
        """
        When not run by an executor in a distributed environment the default behaviour is for
        :class:`PartitionedModel`s to behave like normal :class:`Model`s but (TODO) using
        multiprocessing for local parallel execution.

        Subtasks could override this local execution.
        """
        partition_option = self.partition_plea()

        # this will become parallel multiprocess later
        assert partition_option.minimum == 1

        sub_task_method_defs = self.partition_slice(1)
        assert len(sub_task_method_defs) == 1

        method_name = sub_task_method_defs[0][0]
        method_kwargs = sub_task_method_defs[0][1]

        sub_task_method = getattr(self, method_name)
        subtask_return_value = sub_task_method(**method_kwargs)

        self.partition_subtask_complete(subtask_kwargs=method_kwargs,
                                        subtask_return_value=subtask_return_value)

        self.partition_complete()
