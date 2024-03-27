from dataclasses import asdict, dataclass, field
import json
from typing import Any, Optional


class AbstractTaskMessage:
    """
    Subtasks of an :class:`ayeaye.Model` could be executed locally or across a distributed system.
    Subclass of `AbstractTaskMessage` represent a proposed task or outputs from running a task.
    Outputs are relayed back to the originating model.

    This is a mixin class which is only intended to be used alongside a `dataclass`
    """

    def to_json(self):
        """
        Helper to serialise. See :func:`task_message_factory` to de-serialise.

        @return: (str)
        """
        d = {
            "type": self.__class__.__name__,
            "payload": asdict(self),
        }
        return json.dumps(d)


@dataclass
class TaskPartition(AbstractTaskMessage):
    model_cls: Any  # Not really any but a `Class`; this won't serialise to JSON
    method_name: str
    method_kwargs: dict
    model_construction_kwargs: dict = field(default_factory=dict)
    partition_initialise_kwargs: dict = field(default_factory=dict)


@dataclass
class TaskComplete(AbstractTaskMessage):
    method_name: str
    method_kwargs: dict
    return_value: Any


@dataclass
class TaskFailed(AbstractTaskMessage):
    model_class_name: str
    model_construction_kwargs: dict
    partition_initialise_kwargs: dict
    method_name: str
    method_kwargs: dict
    resolver_context: dict
    exception_class_name: str
    traceback: list


@dataclass
class TaskLogMessage(AbstractTaskMessage):
    msg: str
    level: Optional[str] = "INFO"  # TODO enum the usual posix levels


# could introspect but this will do
task_message_types = {cls.__name__: cls for cls in [TaskComplete, TaskFailed, TaskLogMessage]}


def task_message_factory(json_str):
    """
    Factory method to return a dataclass instance which is a subclass of :class:`AbstractTaskMessage`.

    @param json_str: (str). Expected to be the output from :meth:`AbstractTaskMessage.to_json`
    @raise Exception, probably ValueError: is `json_str` is invalid.
    """
    d = json.loads(json_str)

    message_cls = task_message_types.get(d["type"])

    if message_cls is None:
        raise ValueError(f'Unknown message type \'{d["type"]}\'')

    message = message_cls(**d["payload"])
    return message
