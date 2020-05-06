# import ayeaye
from ayeaye.connectors.base import BaseConnector
from ayeaye.model import Model
from inspect import isclass


class ModelsConnector(BaseConnector):

    def __init__(self, models):
        """
        Connector to run :class:`ayeaye.Models`.

        For args: @see :class:`connectors.base.BaseConnector`

        additional args for ModelsConnector
         models (class, list, set or callable) all of which are :class:`ayeaye.Models`. They aren't
                    instances of the class but the class itself.
        """
        super().__init__()

        invalid_construction_msg = ("models must (class, list, set or callable). All of which "
                                    "result in one or more :class:`ayeaye.Models` classes (not "
                                    "instances)."
                                    )

        # validate and prepare
        if isclass(models) and issubclass(models, Model):
            models = [models, ]

        if callable(models):
            # TODO
            raise NotImplementedError("TODO")

        elif isinstance(models, (list, set)):
            if not all([isclass(m) and issubclass(m, Model) for m in models]):
                raise ValueError(invalid_construction_msg)
            self.models = models

        else:
            raise ValueError(invalid_construction_msg)
