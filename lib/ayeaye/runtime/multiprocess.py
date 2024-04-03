"""
Run :class:`ayeaye.PartitionedModel` models across multiple operating system processes.
"""
from multiprocessing import Process, Queue
import sys
import traceback

from ayeaye.connect_resolve import connector_resolver
from ayeaye.runtime.task_message import TaskComplete, TaskFailed, TaskLogMessage, TaskPartition


class QueueLogger:
    """
    Redirect log messages from a sub-task's :class:`Process` to the parent's :meth:`log`.
    """

    def __init__(self, log_prefix, log_queue):
        self.log_prefix = log_prefix
        self.log_queue = log_queue

    def write(self, msg):
        # TODO structured logging
        log_serialised = TaskLogMessage(msg=msg)
        self.log_queue.put(log_serialised)


class AbstractProcessPool:
    def run_subtasks(self, sub_tasks, context_kwargs=None, processes=None):
        """
        Generator yielding messages from subtasks.

        Messages are a subclass of :class:`AbstractTaskMessage`.

        A subtask is considered complete when it yields either :class:`TaskComplete` or
        :class:`TaskFailed`. It can yield many :class:`TaskLogMessage`

        @param sub_tasks: list of :class:`TaskPartition` objects
            each item defines a subtask to execute in a worker process

        @param processes: (int or None)
            optionally tell each worker the total number of worker processes. This makes it
            easy for workers do choose a subset of records. This number can't exceed
            `self.max_processes`. If it does a ValueError exception is raised. An exception seems
            brutal but is more intuitive than auto-adjusting the actual number of processes.
            If None is given, `self.max_processes` is used.

        @param context_kwargs: (dict)
            connector_resolver context key value pairs.
            The connector resolver is a 'globally accessible' object used to resolve engine_urls.
            It takes key value pairs and callables. This argument is just for the former.
            @see :class:`ayeaye.ayeaye.connect_resolve.ConnectorResolver`

        """
        raise NotImplementedError("Must be implemented by subclasses")


class LocalProcessPool(AbstractProcessPool):
    """
    Like :class:`multiprocessing.Pool` but with instances that persist for as long as the process.

    A worker :class:`multiprocessing.Process` runs methods on an :class:`ayeaye.Model`s instance.
    The model instance is instantiated when the process starts, from there on just the methods
    requested to run the sub-task are called.

    It's frustrating that this can't be done with :class:`multiprocessing.Pool`. Please let me
    know if you can see a way with `Pool`.

    This subclass of :class:`AbstractProcessPool` doesn't re-try failed tasks; it simply passes
    a :class:`TaskFailed` message to the calling model. @see :method:`run_subtasks.`
    """

    def __init__(self, max_processes):
        """
        @param max_processes: (int) upper limit for number of concurrent processes.
        """
        self.max_processes = max_processes
        self.proc_table = None

    def __del__(self):
        """
        Clean up child processes if they are left orphaned. This can happen if an exception
        within a process isn't handled correctly and would result in exceptions being
        dumped.
        """

        if self.proc_table:
            # terminate any processes that are still alive
            for proc in self.proc_table:
                if proc.is_alive():
                    proc.terminate()

            # wait for processes to end
            for proc in self.proc_table:
                proc.join()

    def run_subtasks(self, sub_tasks, context_kwargs=None, processes=None):
        """
        Generator yielding instances that are a subclass of :class:`AbstractTaskMessage`. These
        are from subtasks.

        @see doc. string in :meth:`AbstractProcessPool.run_subtasks`
        """
        if processes is None:
            processes = self.max_processes

        if processes > self.max_processes:
            raise ValueError(f"{processes} processes passed, max set to {self.max_processes}")

        subtasks_count = len(sub_tasks)
        context_kwargs = context_kwargs or {}

        subtasks_queue = Queue()
        return_values_queue = Queue()

        self.proc_table = []
        for proc_id in range(processes):
            proc = Process(
                target=LocalProcessPool.run_model,
                kwargs={
                    "worker_id": proc_id,
                    "total_workers": processes,
                    "subtasks_queue": subtasks_queue,
                    "returns_queue": return_values_queue,
                    "context_kwargs": context_kwargs,
                },
            )
            self.proc_table.append(proc)

        for proc in self.proc_table:
            # daemon for the purpose of child processes being terminated when the parent terminates.
            # proc.daemon = True
            proc.start()

        for sub_task in sub_tasks:
            subtasks_queue.put(sub_task)

        # instruct worker process to terminate
        for _ in range(processes):
            subtasks_queue.put(None)

        completed_procs = 0
        while completed_procs < subtasks_count:
            task_message = return_values_queue.get()

            if isinstance(task_message, (TaskComplete, TaskFailed)):
                completed_procs += 1

            # could be a log message or sub-task completed notification
            yield task_message

        for proc in self.proc_table:
            proc.join()

    @staticmethod
    def run_model(
        worker_id,
        total_workers,
        subtasks_queue,
        returns_queue,
        context_kwargs,
    ):
        """
        @param worker_id: (int)
            unique number assigned in ascending order to workers as they start

        @param total_workers: (int)
            Number of workers in pool or None for dynamic workers

        @param subtasks_queue: :class:`multiprocessing.Queue` object
            subtasks are defined in :class:`TaskPartition` objects; each subtask is an item read
            from this queue.

        @param returns_queue: :class:`multiprocessing.Queue` object
            Multiplex data from the subtask back to the caller (i.e. the instance that made the
            sub-tasks).
            Format for the queue is the output from :meth:`to_json` from any subclass of
            :class:`AbstractTaskMessage`.

        @param context_kwargs: (dict)
            Output from :meth:`connect_resolve.ConnectorResolver.capture_context` - but without
            the 'mapper' key.
            key/values that are made available to the model. These are likely to be taken from
            `self` and passed to this static method as it alone runs in the :class:`Process`.
            @see :class:`connect_resolve.ConnectorResolver`
        """
        # brutal reset but needs a test to ensure threads don't share

        # Depending on OS's fork() the parent process's memory could be available to this Process. Clear
        # all context so only `context_kwargs` is used.
        # For more detail see unittest TestRuntimeMultiprocess.test_resolver_context_not_inherited
        connector_resolver.brutal_reset()

        with connector_resolver.context(**context_kwargs):
            # send logs from the sub-task running in separate Process back to the parent down the queue
            q_logger = QueueLogger(log_prefix=f"Task ({worker_id})", log_queue=returns_queue)

            while True:
                task_message = subtasks_queue.get()

                # None on queue means end process as all work has been completed
                if task_message is None:
                    break

                assert isinstance(task_message, TaskPartition)

                if task_message.method_kwargs is None:
                    task_message.method_kwargs = {}

                model = task_message.model_cls(**task_message.model_construction_kwargs)
                model.set_logger(q_logger)

                # switch off STDOUT as I'm pretty sure it shouldn't be used by a process that has forked as
                # only the parent is joined to a terminal.
                model.log_to_stdout = False

                model.runtime.worker_id = worker_id
                model.runtime.total_workers = total_workers

                model.partition_initialise(**task_message.partition_initialise_kwargs)

                # TODO - :meth:`log` for the worker processes should be connected back to the parent
                # with a queue or pipe and it shouldn't be using stdout

                # TODO - supply the connector_resolver context

                sub_task_method = getattr(model, task_message.method_name)

                try:
                    subtask_return_value = sub_task_method(**task_message.method_kwargs)
                    task_msg = TaskComplete(
                        method_name=task_message.method_name,
                        method_kwargs=task_message.method_kwargs,
                        return_value=subtask_return_value,
                    )

                except Exception as e:
                    # TODO - this is a bit rough
                    _e_type, _e_value, e_traceback = sys.exc_info()
                    traceback_ln = []
                    tb_list = traceback.extract_tb(e_traceback)
                    for filename, line, funcname, text in tb_list:
                        t = f"Traceback:  File[{filename}] Line[{line}] Text[{text}]"
                        traceback_ln.append(t)

                    task_msg = TaskFailed(
                        model_class_name=task_message.model_cls.__name__,
                        model_construction_kwargs=task_message.model_construction_kwargs,
                        partition_initialise_kwargs=task_message.partition_initialise_kwargs,
                        method_name=task_message.method_name,
                        method_kwargs=task_message.method_kwargs,
                        resolver_context=context_kwargs,
                        exception_class_name=str(type(e)),
                        traceback=traceback_ln,
                    )

                returns_queue.put(task_msg)

                model.close_datasets()
