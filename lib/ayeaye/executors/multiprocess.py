"""
Run :class:`ayeaye.PartitionedModel` models across multiple operating system processes.
"""
from multiprocessing import Process, Queue


class ProcessPool:
    """
    Like :class:`multiprocessing.Pool` but with instances that persist for as long as the process.

    A worker :class:`multiprocssing.Process` runs methods of an :class:`ayeaye.Model`s instance.
    The model instance is instantiated when the process starts, from there on just the methods
    requested to run the sub-task are called.

    It's frustrating that this can't be done with :class:`multiprocessing.Pool`. Please let me
    know if you can see a way with `Pool`.
    """

    def __init__(self, processes=None, model_initialise=None):
        """
        Indicate how many worker processes to create with either-

        @param processes: (int)
            number of worker processes

            OR

        @param model_initialise (list of args (list) or kwargs (dict))
            Each item in this list is passed as either *args or **kwargs to the Aye-aye model's
            :method:`partition_initialise`.
        """
        assert (processes is None) != (model_initialise is None), "Mutually exclusive arguments."

        self.processes = processes
        self.model_initialise = model_initialise

    @staticmethod
    def run_model(ayeaye_model_cls, subtask_kwargs_queue, return_values_queue, initialise):
        """
        @param ayeaye_model_cls: subclass of :class:`ayeaye.PartitionedModel`
            Class, not object/instance.
            This will be instantiated without arguments and subtasks will be methods executed on
            this instance.

        @param subtask_kwargs_queue: :class:`multiprocessing.Queue` object
            subtasks are defined by the (method_name, kwargs) (str, dict) items read from this queue

        @param return_values_queue: :class:`multiprocessing.Queue` object
            method_name, method_kwargs, subtask_return_value from running are sent back to the
            calling the subtask along this queue.

        @param initialise: None, dict or list
            args or kwargs for Aye-aye model's :method:`partition_initialise`
        """

        model = ayeaye_model_cls()

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

            # TODO - :method:`log` for the worker processes should be connected back to the parent
            # with a queue or pipe and it shouldn't be using stdout

            # TODO - supply the connector_resolver context

            # TODO - handle exceptions

            sub_task_method = getattr(model, method_name)
            subtask_return_value = sub_task_method(**method_kwargs)
            return_values_queue.put((method_name, method_kwargs, subtask_return_value))

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
            args or kwargs for Aye-aye model's :method:`partition_initialise`.
            Each item in this list is used to initialise a worker process.
        """
        subtasks_count = len(tasks)

        # Optionally, a model can pass initialisation variables to workers
        if initialise is None:
            worker_init = [None for _ in range(self.processes)]
        else:
            if len(initialise) != self.processes:
                raise ValueError("The numeber of worker 'initialise' items doesn't match number of workers.")
            worker_init = initialise

        subtask_kwargs_queue = Queue()
        return_values_queue = Queue()

        proc_table = []
        for proc_id in range(self.processes):
            proc = Process(
                target=ProcessPool.run_model,
                args=(model_cls, subtask_kwargs_queue, return_values_queue, worker_init[proc_id]),
            )
            proc_table.append(proc)

        for proc in proc_table:
            proc.daemon = True
            proc.start()

        for sub_task in tasks:
            subtask_kwargs_queue.put(sub_task)

        for _ in range(self.processes):
            subtask_kwargs_queue.put((None, None))

        for _ in range(subtasks_count):
            yield return_values_queue.get()

        for proc in proc_table:
            proc.join()
