from collections import defaultdict
import copy
from dataclasses import dataclass
from inspect import isclass
import itertools

from ayeaye.connectors.base import AccessMode
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
            for dataset in model_cls.connects().values():
                if dataset.access in [AccessMode.READ, AccessMode.READWRITE]:
                    node.sources.add(dataset)
                    all_sources.add(dataset)

                if dataset.access in [AccessMode.WRITE, AccessMode.WRITE]:
                    node.targets.add(dataset)
                    all_targets.add(dataset)

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

        @dataclass
        class DatasetConnections:
            consumers: set  # models that read from this dataset
            producers: set  # models that write to this dataset

        all_targets, all_sources, nodes = self._base_graph()
        leaf_sources = all_sources - all_targets
        leaf_targets = all_targets - all_sources

        dataset_model_map = defaultdict(lambda: DatasetConnections(consumers=set(), producers=set()))
        all_models = set()
        for node in nodes.values():

            all_models.add(node.model_cls)

            for dataset in node.targets:
                dataset_model_map[dataset].producers.add(node.model_cls)

            for dataset in node.sources:
                dataset_model_map[dataset].consumers.add(node.model_cls)

        for dataset in leaf_sources:
            dataset_model_map[dataset].producers.add(None)

        for dataset in leaf_targets:
            dataset_model_map[dataset].consumers.add(None)

        def traverse_graph(model_cls, models_visited):
            """
            recurse to build graph of everything connected to model_cls
            @return: (models_visited, ModelGraph) - (set, obj)
            """
            if model_cls is None:
                return models_visited, ModelGraph(model=None, sources=[], targets=[])

            models_visited.add(model_cls)

            targets = []
            for ds in nodes[model_cls].targets:
                for model in dataset_model_map[ds].consumers:

                    if model not in models_visited:
                        models_visited, model_graph = traverse_graph(model, models_visited)
                        targets.append((model_graph, ds))

            sources = []
            for ds in nodes[model_cls].sources:
                for model in dataset_model_map[ds].producers:

                    if model not in models_visited:
                        models_visited, model_graph = traverse_graph(model, models_visited)
                        sources.append((model_graph, ds))

            return models_visited, ModelGraph(model=model_cls, sources=sources, targets=targets)

        # put each node into a ModelGraph
        sub_graphs = []
        models_visited = set()

        while True:

            unvisited_models = list(all_models - models_visited)

            if len(unvisited_models) == 0:
                break

            model_cls = unvisited_models[0]
            # models_visited.add(model_cls)
            models_visited_traverse, model_graph = traverse_graph(model_cls, models_visited)
            sub_graphs.append(model_graph)

            # find all models visited in that graph
            models_visited.update(models_visited_traverse)

        graph_set = []
        for graph in sub_graphs:
            sub_graph_edges = set()
            for dataset in graph.all_datasets:

                # every model that writes to the dataset
                for model_a in dataset_model_map[dataset].producers:
                    # every model that reads from the dataset
                    for model_b in dataset_model_map[dataset].consumers:
                        mge = ModelGraphEdge(model_a=model_a, model_b=model_b, dataset=dataset)
                        sub_graph_edges.add(mge)

            graph_set.append(sub_graph_edges)

        return graph_set


@dataclass
class ModelGraphEdge:
    """
    A representation of the :class:`ModelGraph` is a list of edges.

    Two :class:`ayeaye.Model`s and a 'dataset' (:class:`Connect` or `DataConnector`) is an edge.
    This was an arbitrary decision, it could have been two datasets and a model.
    """

    model_a: Model
    model_b: Model
    dataset: object

    def __hash__(self):
        return hash((self.model_a, self.model_b, self.dataset))


@dataclass
class ModelGraph:
    """
    Connections between :class:`ayeaye.Model` objects..

    Each node has a model and links to other nodes via edges .sources and .targets.
    Each item in .sources and .targets lists are tuples (ModelGraph, Connect/DataConnector).
    """

    model: Model
    sources: list  # graph edge of (ModelGraph, Connect/DataConnector)
    targets: list  # graph edge of (ModelGraph, Connect/DataConnector)

    def traverse_tree(self):
        """
        generator yielding all edges (ModelGraph, dataset) in graph.
        """
        for model_graph, dataset in itertools.chain(self.sources, self.targets):
            yield model_graph, dataset
            yield from model_graph.traverse_tree()

    @property
    def datasets(self):
        """
        @return: set of all datasets/connections from sources and targets connected to this node.
        """
        source_ds = [dataset for _, dataset in self.sources]
        target_ds = [dataset for _, dataset in self.targets]
        return set(source_ds + target_ds)

    @property
    def all_datasets(self):
        """
        @return: set of all datasets connected within this graph. i.e. does contain leaf nodes
        """
        datasets = self.datasets
        for _, dataset in self.traverse_tree():
            datasets.add(dataset)

        return datasets


class VisualiseModels:
    "Experiment to visualise run order and data provenance for a :class:`ModelCollection` instance"

    def __init__(self, model_collection):
        """
        @param model_collection - instance of :class:`ModelCollection`
        """

    def mermaid_run_order(self):
        """
        @return (str)
            mermaid format (see https://github.com/mermaid-js/mermaid#readme) to visualise model
            execution order.
        """
        # gantt
        #     section Section
        #     Completed :done,    des1, 2014-01-06,2014-01-08
        #     Active        :active,  des2, 2014-01-07, 3d
        #     Parallel 1   :         des3, after des1, 1d
        #     Parallel 2   :         des4, after des1, 1d
        #     Parallel 3   :         des5, after des3, 1d
        #     Parallel 4   :         des6, after des4, 1d
        #

        out = ["gantt"]

        return "\n".join(out)
