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

    def __init__(self, processes):
        """
        @param processes: (int)
            number of worker processes
        """
        self.processes = processes

    @staticmethod
    def run_model(ayeaye_model_cls, subtask_kwargs_queue, return_values_queue):
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
        """

        model = ayeaye_model_cls()
        model.partition_initialise()

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

    def run_subtasks(self, model_cls, tasks):
        """
        Generator yielding (method_name, method_kwargs, subtask_return_value) from completed
        subtasks.

        @param ayeaye_model_cls: subclass of :class:`ayeaye.PartitionedModel`
            Class, not object/instance.
            This will be instantiated without arguments and subtasks will be methods executed on
            this instance.

        @param tasks: list of (method_name, method_kwargs) (str, dict)
            each item defines a subtask to execute in a worker process
        """

        subtask_kwargs_queue = Queue()
        return_values_queue = Queue()

        proc_table = []
        for _ in range(self.processes):
            proc = Process(target=ProcessPool.run_model, args=(model_cls, subtask_kwargs_queue, return_values_queue))
            proc_table.append(proc)

        for proc in proc_table:
            proc.daemon = True
            proc.start()

        for sub_task in tasks:
            subtask_kwargs_queue.put(sub_task)

        for _ in range(self.processes):
            subtask_kwargs_queue.put((None, None))

        for _ in range(len(tasks)):
            yield return_values_queue.get()

        for proc in proc_table:
            proc.join()
