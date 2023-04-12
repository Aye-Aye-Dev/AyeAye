from inspect import isclass

from .abstract_modifier import AbstractEngineTypeModifier
from .smart_open_modifier import SmartOpenModifier


def engine_type_modifier_factory(connector_cls, modifier_labels):
    """
    Return a single modifier that supports all of the `modifier_labels` and can work with the
    `connector_cls`.

    Behind the scenes this could use composition, dynamic inheritance etc. For now, it's
    simple - the returned modifier simultaneously supports all of the specified modifier_labels.

    @param connector_cls: (subclass of :class:`DataConnector`)
        The target data connector for the modifier.

    @param modifier_labels (list of str):
        Named properties that must all be supported by the returned modifier.
        e.g. ['s3', 'gz']

    @return (subclass of :class:`AbstractEngineTypeModifier`)
    """

    for modifier_cls in connector_modifiers_registry.registered_modifiers:
        if modifier_cls.provides_support(connector_cls, modifier_labels):
            return modifier_cls

    mod_labels = "+".join(modifier_labels)
    msg = (
        f"The combination of {connector_cls.__name__} and {mod_labels} engine type modifiers "
        "isn't supported."
    )
    raise NotImplementedError(msg)


class ConnectorModifiersPluginsRegistry:
    """
    A modifier adds capabilities to a connector, for example, transparent compression.
    """

    def __init__(self):
        self.registered_modifiers = []  # list of classes, not instances - publicly readable
        self.reset()

    def reset(self):
        "set registered connectors to just those that are built in"
        self.registered_modifiers = [
            SmartOpenModifier,
        ]

    def register_connector(self, modifier_cls):
        """
        Add a private modifier to the dataset connection discovery process.
        @param modifier_cls (subclass of :class:`AbstractEngineTypeModifier`):
        """
        if not isclass(modifier_cls) or not issubclass(modifier_cls, AbstractEngineTypeModifier):
            msg = "'connector_cls' should be a class (not object) and subclass of AbstractEngineTypeModifier"
            raise TypeError(msg)

        # MAYBE - a mechanism to specify the position/priority of the class
        self.registered_connectors.append(modifier_cls)


# global registry
connector_modifiers_registry = ConnectorModifiersPluginsRegistry()
