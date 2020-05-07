import copy
from inspect import isclass

from ayeaye.connectors.base import AccessMode, BaseConnector
from ayeaye.model import Model
from ayeaye.pinnate import Pinnate


class ModelsConnector(BaseConnector):

    def __init__(self, models):
        """
        Connector to run :class:`ayeaye.Models`.

        For args: @see :class:`connectors.base.BaseConnector`

        additional args for ModelsConnector
         models (class, list, set or callable) all of which are :class:`ayeaye.Models`. They aren't
                    instances of the class but the class itself. A *list* is an explicit ordering
                    that the models should be run in. Models in *set* will be examined to see
                    dataset dependencies and these will be used to determine the model run order.

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

    def _resolve_run_order(self):
        """
        Use the dataset connections in each model to determine the dependencies between models and
        therefore the order to run them in.

        @return: (Pinnate) with attributes:
                    leaf_sources (set of datasets) - read but not written to
                    leaf_targets (set of datasets) - written to but not read - end goal of model
                    run_order (list of sets of nodes) - All models in a set can be run in parallel.
                            Each set must be complete before the next set is run.

                    'datasets' as subclasses of :class:`Connect`
                    `nodes` are type :class:`Pinnate` with attributes model_cls, model_name,
                        targets and sources
        """
        # This algorithm is a bit overly simplistic and sub-optimal. The return is a list of sets
        # with all models in set needing to be run. But the set is actually the models that were
        # ready to run when the prior models are complete. So there could be subsequent models that
        # are only waiting on a subset of each set.

        # Find all the datasets in all the model and classify datasets as 'sources' (READ access)
        # and 'targets' (WRITE access) or READWRITE for both.
        all_targets = set()
        all_sources = set()
        nodes = {}
        for model_cls in self.models:
            # TODO find ModelConnectors and recurse into those
            model_name = model_cls.__name__
            if model_name in nodes:
                raise ValueError(f"Duplicate node found: {model_name}")

            node = Pinnate({'model_cls': model_cls,
                            'model_name': model_name,
                            'targets': set(),
                            'sources': set()
                            })
            for dataset in model_cls.connects().values():
                if dataset.access in [AccessMode.READ, AccessMode.READWRITE]:
                    node.sources.add(dataset)
                    all_sources.add(dataset)

                if dataset.access in [AccessMode.WRITE, AccessMode.WRITE]:
                    node.targets.add(dataset)
                    all_targets.add(dataset)

            nodes[model_name] = node

        leaf_sources = all_sources - all_targets
        leaf_targets = all_targets - all_sources

        completed = copy.copy(leaf_sources)
        run_order = []
        while len(nodes) > 0:
            loop_ready = set()
            for node in nodes.values():
                if node.sources.issubset(completed):
                    loop_ready.add(node)

            if len(loop_ready) == 0:
                msg = "The set of models can't be built into a single acyclic graph."
                raise ValueError(msg)

            # nodes in loop_ready have been visited add their edges to the completed map
            run_order.append(loop_ready)
            for ready_node in loop_ready:
                node = nodes[ready_node.model_name]
                completed = completed.union(node.targets)
                del nodes[ready_node.model_name]

        p = Pinnate({'leaf_sources': leaf_sources,
                     'leaf_targets': leaf_targets,
                     'run_order': run_order
                     })
        return p
