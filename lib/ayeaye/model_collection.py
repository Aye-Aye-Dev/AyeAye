from collections import defaultdict
import copy
from dataclasses import dataclass
from inspect import isclass
import itertools

from ayeaye.connectors.base import AccessMode
from ayeaye.connectors.multi_connector import MultiConnector
from ayeaye.model import Model
from ayeaye.pinnate import Pinnate


class ModelCollection:
    def __init__(self, models):
        """
        Build graphs from a group of subclasses of :class:`ayeaye.Models`

        @param models (class, list, set or callable)
            All of which are :class:`ayeaye.Models`. They aren't instances of the class but the
            class itself. A *list* is an explicit ordering that the models should be run in. Models
            in *set* will be examined to see dataset dependencies and these will be used to
            determine the model run order.
        """
        super().__init__()

        invalid_construction_msg = (
            "models must be a class, list, set or callable. All of which "
            "result in one or more :class:`ayeaye.Models` classes (not "
            "instances)."
        )

        # validate and prepare
        if isclass(models) and issubclass(models, Model):
            models = [
                models,
            ]

        if callable(models):
            # TODO
            raise NotImplementedError("TODO")

        elif isinstance(models, (list, set)):
            if not all([isclass(m) and issubclass(m, Model) for m in models]):
                raise ValueError(invalid_construction_msg)
            self.models = models

        else:
            raise ValueError(invalid_construction_msg)

    def __len__(self):
        return len(self.models)

    def __iter__(self):
        """
        yield model classes. No ordering when models are a set but does honour order when models
        is a list
        """
        yield from self.models

    def _base_graph(self):
        """Find all the datasets in all the models and classify datasets as 'sources' (READ access)
        and 'targets' (WRITE access) or READWRITE for both.

        @return: (targets, sources, nodes) (set, set, dictionary)

            node - 'datasets' are a subclasses of :class:`Connect` or :class:`DataConnector`

            all_targets - one or more models writes to these
            all_sources - one or more models read from these
            nodes - key is model class, value is Pinnate with .model_cls, .model_name, .targets, .sources
        """
        all_targets = set()
        all_sources = set()
        nodes = {}
        for model_cls in self.models:
            # TODO find ModelConnectors and recurse into those
            model_name = model_cls.__name__
            if model_name in nodes:
                raise ValueError(f"Duplicate node found: {model_name}")

            node = Pinnate({"model_cls": model_cls, "model_name": model_name, "targets": set(), "sources": set()})

            # as instantiated model
            # model_obj = model_cls()

            for class_attrib_label, ds_connector in model_cls.connects().items():
                # for class_attrib_label, ds_connector in model_obj.datasets().items():

                # if isinstance(ds_connector, Connect):
                #
                #     if isinstance(ds_connector, list):
                #         # experiment with Connector prior to instantiation as MultiConnector
                #         m_connectors = [Connect()]

                if isinstance(ds_connector, MultiConnector):
                    m_connectors = ds_connector.data
                else:
                    m_connectors = [ds_connector]

                for dataset_connector in m_connectors:
                    dataset_container = ModelDataset(
                        model_attrib_label=class_attrib_label, connector=dataset_connector
                    )

                    if dataset_connector.access in [AccessMode.READ, AccessMode.READWRITE]:
                        node.sources.add(dataset_container)
                        all_sources.add(dataset_container)

                    if dataset_connector.access in [AccessMode.WRITE, AccessMode.WRITE]:
                        node.targets.add(dataset_container)
                        all_targets.add(dataset_container)

            nodes[model_cls] = node

        return all_targets, all_sources, nodes

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
        all_targets, all_sources, nodes = self._base_graph()

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
                node = nodes[ready_node.model_cls]
                completed.update(node.targets)
                del nodes[ready_node.model_cls]

        p = Pinnate({"leaf_sources": leaf_sources, "leaf_targets": leaf_targets, "run_order": run_order})
        return p

    def run_order(self):
        """
        @return: (list) items in the list are either-
                    (i) sets of subclasses of :class:`ayeaye.Model`
                    OR
                    (ii) lists of sets as per (i)

        All models in a set can be run in parallel. Each set must be complete before the next set
        in the list is run.
        """
        if isinstance(self.models, list):
            # models in 'list' mode are wrapped as single item sets because all items in a set must
            # complete before next item in a list is run.
            return [{m} for m in self]

        elif isinstance(self.models, set):
            # _resolve_run_order returns leaf nodes and the run order is built using Pinnate
            # instances as nodes. Remove all this with to return a simple run order.
            resolved_order = self._resolve_run_order()

            def simplify_nodes(n):
                if isinstance(n, Pinnate):
                    return n.model_cls
                if isinstance(n, (set, list)):
                    rx = [simplify_nodes(nx) for nx in n]
                    if isinstance(n, set):
                        return set(rx)
                    return rx

            return [simplify_nodes(r) for r in resolved_order.run_order]

        else:
            raise ValueError("Unknown models container.")

    def dataset_provenance(self):
        """
        self.models is a set of :class:`ayeaye.Model`s. These might all be interconnected or they
        may form multiple graphs.

        @return: set (graphs) of sets (edges)
            graphs are a set of edges
            edges are :class:`ModelGraphEdge` objects.
        """
        all_targets, all_sources, nodes = self._base_graph()
        leaf_sources = all_sources - all_targets
        leaf_targets = all_targets - all_sources

        # lookup to sources for each dataset
        dataset_sources = defaultdict(list)
        for node in nodes.values():
            for dataset_container in node.sources:
                dataset_sources[dataset_container].append(node.model_cls)

        def dataset_source(target_dataset_container):
            "generator yielding model classes"
            for source_model_cls in dataset_sources[target_dataset_container]:

                for source_dataset_container in nodes[source_model_cls].sources:
                    if source_dataset_container == target_dataset_container:
                        yield source_model_cls, source_dataset_container

        edge_set = set()
        for node in nodes.values():

            # print(node.model_cls.__name__, len(node.targets))

            for dataset_container_a in node.sources:

                if dataset_container_a in leaf_sources:
                    dataset_label = dataset_container_a.model_attrib_label
                    mge = ModelGraphEdge(model_a=None, model_b=node.model_cls, dataset_label=dataset_label)
                    edge_set.add(mge)

            for dataset_container_a in node.targets:

                if dataset_container_a in leaf_targets:
                    dataset_label = dataset_container_a.model_attrib_label
                    mge = ModelGraphEdge(model_a=node.model_cls, model_b=None, dataset_label=dataset_label)
                    edge_set.add(mge)

                for model_b, dataset_container_b in dataset_source(dataset_container_a):

                    # print(node.model_cls, dataset_container_a, model_b, dataset_container_b)

                    dataset_label = dataset_container_a.model_attrib_label
                    # models might each use different attrib names
                    if dataset_container_b is not None and dataset_label != dataset_container_b.model_attrib_label:
                        dataset_label += " / " + dataset_container_b.model_attrib_label

                    mge = ModelGraphEdge(model_a=node.model_cls, model_b=model_b, dataset_label=dataset_label)
                    edge_set.add(mge)

        # TODO - are sub graphs needed?
        graph_set = list()
        graph_set.append(list(edge_set))
        return graph_set


@dataclass
class ModelDataset:
    """
    Container for attributes associated with a dataset that belongs to a model.

    A dataset could be a :class:`ayeaye.Connect` or it could have been resolved to a subclass of
    :class:`DataConnector`. Hold these (todo) and other associated info about the dataset.
    """

    model_attrib_label: str  # name of class variable in parent model
    connector: object  #  ayeaye.Connect

    def __hash__(self):
        """
        super critical to graphs being able to build is equating two datasets are the same thing
        without the name (i.e. model_attrib_label) mattering.
        """
        return self.connector.__hash__()

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.connector == other.connector
        return False


@dataclass
class ModelGraphEdge:
    """
    A representation of the :class:`ModelGraph` is a list of edges.

    Two :class:`ayeaye.Model`s and a 'dataset' (:class:`Connect` or `DataConnector`) is an edge.
    This was an arbitrary decision, it could have been two datasets and a model.
    """

    model_a: Model
    model_b: Model
    dataset_label: str

    def __hash__(self):
        return hash((self.model_a, self.model_b, self.dataset_label))


class VisualiseModels:
    "Experiment to visualise run order and data provenance for a :class:`ModelCollection` instance"

    def __init__(self, model_collection):
        """
        @param model_collection - instance of :class:`ModelCollection`
        """
        self.model_collection = model_collection

    def mermaid_data_provenance(self):
        """
        @return (str)
            mermaid format (see https://github.com/mermaid-js/mermaid#readme) to visualise model's
            data dependencies.
        """

        def _leaf_label():
            "return name (str) for a leaf node"
            r = 0
            while True:
                yield f"leaf_{r}([ ])"
                r += 1

        # no idea if leaves are the same dataset or not
        leaf_label = _leaf_label()

        graphset = self.model_collection.dataset_provenance()
        if len(graphset) == 0:
            return ""

        # if len(graphset) > 1:
        #     raise NotImplementedError("Multiple graphs within one collection of models is not yet implemented!")

        out = ["graph LR"]
        for graph in graphset:
            # graphs don't need to be separate for Mermaid
            for edge in graph:

                model_a = edge.model_a.__name__ if edge.model_a is not None else next(leaf_label)
                model_b = edge.model_b.__name__ if edge.model_b is not None else next(leaf_label)

                edge_fmt = f"{model_a}-->|{edge.dataset_label}| {model_b}"
                out.append(edge_fmt)

        return "\n".join(out)
