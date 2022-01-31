"""
This link between the runtime 'execution' environment and a worker process running a model.

Created on 31 Jan 2022

@author: si
"""


class RuntimeKnowledge:
    """
    This link between the runtime 'execution' environment and a worker process running a model.

    This module will develop to make it possible for a model to introspect and communicate with
    it's environment.

    Available attributes-

    * worker_id (int) - unique integer assigned in ascending order to workers as they start
    * total_workers (int) - Number of workers created in worker group. Is `None` if variable number
                    of workers are being used.
    """

    def __init__(self):
        self.worker_id = None
        self.total_workers = None
