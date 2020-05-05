from ayeaye.connectors.base import BaseConnector


class ModelsConnector(BaseConnector):

    def __init__(self, models):
        """
        Connector to run :class:`ayeaye.Models`.

        For args: @see :class:`connectors.base.BaseConnector`

        additional args for ModelsConnector
         None
        """
        super().__init__()
        self.models = models
