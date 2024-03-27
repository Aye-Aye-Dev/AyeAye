"""
Created on 25 Apr 2023

@author: si
"""


class UnknownEngineType(Exception):
    """
    The engine url either doesn't contain an engine_type or there isn't a registered (subclass of)
    :class:`DataConnector` for the engine_type.

    Engine urls have the format: engine_type://engine_params
    """

    def __init__(self, engine_url):
        """
        @param engine_url: (str)
            The engine_url that contained the unknown engine_type
        """
        super().__init__(f"Unknown engine_type in url: '{engine_url}'")
        self.engine_url = engine_url


class SubTaskFailed(Exception):
    """
    :class:`PartitionedModel` splits a task into sub-tasks and though either local or distributed
    execution runs these sub-tasks independently. This exception is raised on the originating model
    when a sub-task has raised an exception.

    It contains a load of information about the failure within the :attr:`task_fail_message`. See
    :class:`ayeaye.runtime.task_message.TaskFailed` for all the fields.
    """

    def __init__(self, task_fail_message):
        """
        @param task_fail: (:class:`ayeaye.runtime.task_message.TaskFailed`)
        """
        failed_cls_name = task_fail_message.model_class_name
        msg = (
            f"Subtask failed. '{failed_cls_name}.{task_fail_message.method_name}' raised an "
            f"{task_fail_message.exception_class_name} exception."
        )
        trace_str = "\n".join(task_fail_message.traceback)
        msg = msg + "\n" + trace_str

        super().__init__(msg)
        self.task_fail_message = task_fail_message
