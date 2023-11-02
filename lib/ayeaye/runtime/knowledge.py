"""
This link between the runtime 'execution' environment and a worker process running a model.

Created on 31 Jan 2022

@author: si
"""
import os


class RuntimeKnowledge:
    """
    This link between the runtime 'execution' environment and a process running a model.

    This module will develop to make it possible for a model to introspect and communicate with
    it's environment. The process holding an instance of this class could be a worker or
    the main process that partitions the task into sub-tasks for the worker.

    Available attributes-

    * worker_id (int) - unique integer assigned in ascending order to workers as they start
    * total_workers (int) - Number of workers created in worker group. Is `None` if variable number
                    of workers are being used.
    """

    def __init__(self):
        self.worker_id = None
        self.total_workers = None

        # Number of concurrent tasks per CPU
        self.cpu_task_ratio = 2

        # can be set to an absolute limit
        # e.g.
        #   model_instance.runtime.max_concurrent_tasks = 1
        # or by default returns number of CPUs * self.cpu_task ratio
        self._max_concurrent_tasks = None

    @property
    def max_concurrent_tasks(self):
        if self._max_concurrent_tasks is not None:
            # user has set an absolute value
            return self._max_concurrent_tasks

        return os.cpu_count() * self.cpu_task_ratio

    @max_concurrent_tasks.setter
    def max_concurrent_tasks(self, max_tasks):
        """
        Override the system default and set a maximum absolute number of concurrent tasks.

        @param max_tasks: (int)
        """
        self._max_concurrent_tasks = max_tasks
