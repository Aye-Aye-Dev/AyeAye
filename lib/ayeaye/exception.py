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
