"""
Run :class:`ayeaye.PartitionedModel` models across multiple operating system processes.
"""
from enum import Enum
from multiprocessing import Process, Queue

from ayeaye.connect_resolve import connector_resolver


# a single queue supports multiple message types
class MessageType(Enum):
    COMPLETE = "COMPLETE"  # return value when subtask is complete
    LOG = "LOG"  # log message


class QueueLogger:
    """
    Redirect log messages from a sub-task's :class:`Process` to the parent's :meth:`log`.
    """

    def __init__(self, log_prefix, log_queue):
        self.log_prefix = log_prefix
        self.log_queue = log_queue

    def write(self, msg):
        # TODO structured logging
        self.log_queue.put((MessageType.LOG, msg))


class ProcessPool:
    """
    Like :class:`multiprocessing.Pool` but with instances that persist for as long as the process.

    A worker :class:`multiprocessing.Process` runs methods of an :class:`ayeaye.Model`s instance.
    The model instance is instantiated when the process starts, from there on just the methods
    requested to run the sub-task are called.

    It's frustrating that this can't be done with :class:`multiprocessing.Pool`. Please let me
    know if you can see a way with `Pool`.
    """

    def __init__(self, processes=None, model_initialise=None, context_kwargs=None):
        """
        Indicate how many worker processes to create with either-

        @param processes: (int)
            number of worker processes

            OR

        @param model_initialise (list of args (list) or kwargs (dict))
            Each item in this list is passed as either *args or **kwargs to the Aye-aye model's
            :meth:`partition_initialise`.

        @param context_kwargs: (dict)
            connector_resolver context key value pairs.
            The connector resolver is a 'globally accessible' object use to resolve engine_urls.
            It takes key value pairs and callables. This argument is just for the former.
            @see :class:`ayeaye.ayeaye.connect_resolve.ConnectorResolver`
        """
        assert (processes is None) != (model_initialise is None), "Mutually exclusive arguments."

        self.processes = processes
        self.model_initialise = model_initialise
        self.context_kwargs = context_kwargs

    @staticmethod
    def run_model(
        worker_id,
        total_workers,
        ayeaye_model_cls,
        subtask_kwargs_queue,
        returns_queue,
        initialise,
        context_kwargs,
    ):
        """
        @param worker_id: (int)
            unique number assigned in ascending order to workers as they start

        @param total_workers: (int)
            Number of workers in pool or None for dynamic workers

        @param ayeaye_model_cls: subclass of :class:`ayeaye.PartitionedModel`
            Class, not object/instance.
            This will be instantiated without arguments and subtasks will be methods executed on
            this instance.

        @param subtask_kwargs_queue: :class:`multiprocessing.Queue` object
            subtasks are defined by the (method_name, kwargs) (str, dict) items read from this queue

        @param returns_queue: :class:`multiprocessing.Queue` object
            Multiplex data from the subtask back to the caller (i.e. the instance that made the
            sub-tasks).
            Format for the queue is-

            MessageType.LOG, message (str)
            MessageType.COMPLETE, method_name, method_kwargs, subtask_return_value from the completed sub-task

        @param initialise: None, dict or list
            args or kwargs for Aye-aye model's :meth:`partition_initialise`

        @param context_kwargs: (dict)
            Output from :meth:`connect_resolve.ConnectorResolver.capture_context` - so needs the
            'mapper' key with dict. as value.
            key/values that are made available to the model. These are likely to be taken from
            `self` and passed to this static method as it alone runs in the :class:`Process`.
            @see :class:`connect_resolve.ConnectorResolver`
        """
        # brutal reset but needs a test to ensure threads don't share

        # Depending on OS's fork() the parent process's memory could be available to this Process. Clear
        # all context so only `context_kwargs` is used.
        # For more detail see unittest TestRuntimeMultiprocess.test_resolver_context_not_inherited
        connector_resolver.brutal_reset()

        with connector_resolver.context(**context_kwargs["mapper"]):
            model = ayeaye_model_cls()

            # send logs from the sub-task running in separate Process back to the parent down the queue
            q_logger = QueueLogger(log_prefix=f"Task ({worker_id})", log_queue=returns_queue)
            model.set_logger(q_logger)

            # switch off STDOUT as I'm pretty sure it shouldn't be used by a process that has forked as
            # only the parent is joined to a terminal.
            model.log_to_stdout = False

            model.runtime.worker_id = worker_id
            model.runtime.total_workers = total_workers

            init_args = []
            init_kwargs = {}
            if initialise is not None:
                for init_as in initialise:
                    if isinstance(init_as, list):
                        init_args = init_as
                    elif isinstance(init_as, dict):
                        init_kwargs = init_as
                    else:
                        raise ValueError("Unknown initialise variable")

            model.partition_initialise(*init_args, **init_kwargs)

            while True:
                method_name, method_kwargs = subtask_kwargs_queue.get()
                if method_name is None:
                    break

                if method_kwargs is None:
                    method_kwargs = {}

                # TODO - :meth:`log` for the worker processes should be connected back to the parent
                # with a queue or pipe and it shouldn't be using stdout

                # TODO - supply the connector_resolver context

                # TODO - handle exceptions

                sub_task_method = getattr(model, method_name)
                subtask_return_value = sub_task_method(**method_kwargs)
                returns_queue.put(
                    (MessageType.COMPLETE, method_name, method_kwargs, subtask_return_value)
                )

            model.close_datasets()

    def run_subtasks(self, model_cls, tasks, initialise):
        """
        Generator yielding (method_name, method_kwargs, subtask_return_value) from completed
        subtasks.

        @param ayeaye_model_cls: subclass of :class:`ayeaye.PartitionedModel`
            Class, not object/instance.
            This will be instantiated without arguments and subtasks will be methods executed on
            this instance.

        @param tasks: list of (method_name, method_kwargs) (str, dict)
            each item defines a subtask to execute in a worker process

        @param initialise: None, or list of tuples (dict and or list)
            args or kwargs for Aye-aye model's :meth:`partition_initialise`.
            Each item in this list is used to initialise a worker process.
        """
        subtasks_count = len(tasks)

        # Optionally, a model can pass initialisation variables to workers
        if initialise is None:
            worker_init = [None for _ in range(self.processes)]
        else:
            if len(initialise) != self.processes:
                raise ValueError(
                    "The numeber of worker 'initialise' items doesn't match number of workers."
                )
            worker_init = initialise

        context_kwargs = self.context_kwargs or {}

        subtask_kwargs_queue = Queue()
        return_values_queue = Queue()

        proc_table = []
        for proc_id in range(self.processes):
            proc = Process(
                target=ProcessPool.run_model,
                args=(
                    proc_id,
                    self.processes,
                    model_cls,
                    subtask_kwargs_queue,
                    return_values_queue,
                    worker_init[proc_id],
                    context_kwargs,
                ),
            )
            proc_table.append(proc)

        for proc in proc_table:
            proc.daemon = True
            proc.start()

        for sub_task in tasks:
            subtask_kwargs_queue.put(sub_task)

        # instruct worker process to terminate
        for _ in range(self.processes):
            subtask_kwargs_queue.put((None, None))

        completed_procs = 0
        while True:
            multiplexed_return = return_values_queue.get()

            if multiplexed_return[0] == MessageType.COMPLETE:
                completed_procs += 1

            # could be a log message or sub-task completed notification
            yield multiplexed_return

            if completed_procs >= subtasks_count:
                break

        for proc in proc_table:
            proc.join()
