"""
A manifest is "a list of people and goods carried on a ship or plane" but here, it's a list of
datasets.

A common pattern is to bootstrap a batched build by listing all the target files used in the build in a
manifest file. This manifest file is a dataset for any model that works on the target files. 
"""
import copy

from collections import defaultdict
from inspect import ismethod

import ayeaye


class EngineFromManifest:
    """
    Use a subclass of :class:`DataConnector` that supports dictionary access to `.data` as a
    manifest. Use this to build engine_url(s) for passing to another dataset within the
    dataset declarations part of an :class:`ayeaye.Model`.

    JSON is a good choice for the manifest dataset.

    e.g.
    ```
    class ProductsBuild(ayeaye.Model):
        manifest = ayeaye.Connect(engine_url=f"json:///data/products_manifest_{build_id}.json")
        products = ayeaye.Connect(engine_url=EngineFromManifest(manifest, "source_files", "csv"))
    ```

    If the resolver_context had `build_id=20210618` and the file `/data/products_manifest_20210618.json`
    contained

        `{"source_files": "products_inventory_2021.csv"}`

    then ...

    ```
    m = ProductsBuild()
    assert m.products.engine_url == "products_inventory_2021.csv"
    ```
    """

    def __init__(self, manifest_dataset, field_name, engine_type):
        """
        :param manifest_dataset (subclass :class:`DataConnector` object): with .data and dictionary
                access to .data.
        :param field_name (str): field within manifest_dataset.data[field_name]
        :param engine_type (str): prefix to engine_url. e.g. 'json' would give 'json://'
        """
        self.manifest_dataset = manifest_dataset

        self.field_name = field_name
        self.engine_type = engine_type

    def __call__(self):
        """
        When used according to the pattern above this will be called on demand by
        :class:`ayeaye.Connect` when a dataset is accessed so the global connector_resolver will
        have been setup with any context that is needed before reading the manifest_dataset. That
        context wouldn't have been available during construction, hense a callable.
        """
        if isinstance(self.manifest_dataset, ayeaye.Connect):
            # .clone() is to prevent the .Connect being bound to the parent if it's connected
            manifest_dataset = self.manifest_dataset.clone()
        else:
            manifest_dataset = self.manifest_dataset

        # create ephemeral dataset not tied to an ayeaye.Model
        e_url = ayeaye.connector_resolver.resolve(manifest_dataset.engine_url)
        ds = ayeaye.Connect(engine_url=e_url)
        dataset_section = ds.data[self.field_name]

        # :class:`connectors.multi_connector.MultiConnector` vs. single engine_urls connectors
        if isinstance(dataset_section, list):
            return [f"{self.engine_type}://{f}" for f in dataset_section]

        return f"{self.engine_type}://{dataset_section}"


class AbstractManifestMapper:
    """
    A manifest is a dataset containing a list of other datasets.

    It's common for an ETL model to transform or map each of these source datasets into a corresponding
    output dataset(s).

    :class:`AbstractManifestMapper` should be subclassed to act as a mapper between datasets in a
    manifest and output datasets. It is used to declare data connections within an :class:`ayeaye.Model`
    (i.e. the :class`ayeaye.Connect` class variables) and it's used within the model to provide
    maps of input to output datasets.

    For the manifest dataset, use a subclass of :class:`DataConnector` that supports dictionary
    access to `.data`. JSON is a good choice for the manifest dataset.

    Sub classes implement a method for each map and this method is either a generator or returns
    a list of (original_manifest_item, engine_url). [Note - this doesn't strictly need to be an
    engine_url if the mapper is used more generally and not just as a helper for ayeaye.Connect
    as is being demonstrated here].

    For example, using xxxx as the map name:

    ```
    class FileMapper(AbstractManifestMapper):
        def map_xxxx(self):
            return [(f, f"json://{f}") for f in self.manifest_items]
    ```

    makes a method .xxxx() which returns just the engine_urls. It is used in an ayeaye.Connect-

    ```
    class AustralianAnimals(ayeaye.Model):
        animals_manifest = ayeaye.Connect(engine_url="json://{input_data}/animals_manifest.json")
        animals_mapper = FileMapper(animals_manifest)
        all_files = ayeaye.Connect(engine_url=animals_mapper.all_files)
    ```

    And FileMapper will also be an iterator returning a object with attributes linking to the
    engine_urls of all mappers.

    For an example of how to use this see-
    https://github.com/Aye-Aye-Dev/AyeAye/blob/master/examples/manifest_mapper.py

    The tests in :class:`tests.common_pattern.test_manifest.TestManifest` also demonstrate usage.

    Notes-

    This class is a descriptor as it's expected to be used to declare class variables that are then
    used within the instance.

    If the subclass changes the constructor's arguments you must implement the __copy__ method.
    """

    def __init__(self, manifest_dataset, field_name):
        """
        :param manifest_dataset (subclass :class:`DataConnector` object): with .data and dictionary
                access to .data.
        :param field_name (str): field within manifest_dataset.data[field_name]
        """
        self.manifest_dataset_unresolved = manifest_dataset
        self.field_name = field_name

    def __copy__(self):
        c = self.__class__(manifest_dataset=self.manifest_dataset_unresolved,
                           field_name=self.field_name)
        return c

    def __get__(self, instance, instance_class):
        if instance is None:
            # class method called.
            # This means `self` is currently an attribute of the class (so NOT an instance
            # variable).
            #
            # see https://docs.python.org/3/howto/descriptor.html
            return self

        ident = id(self)
        # dynamically create a place in the parent class to hold descriptor objects
        if not hasattr(instance, "_abstract_manifes_mapper_vars"):
            instance._abstract_manifes_mapper_vars = {}

        if ident not in instance._abstract_manifes_mapper_vars:
            instance._abstract_manifes_mapper_vars[ident] = copy.copy(self)

        return instance._abstract_manifes_mapper_vars[ident]

    @property
    def manifest_items(self):
        """
        This will be called on demand after the global connector_resolver has been setup with any
        context that is needed before reading the manifest_dataset. That context wouldn't have been
        available during construction.

        Generator yielding each item in the manifest datasets's target field.
        i.e. list of items in manifest.data[self.field_name]
        """
        if isinstance(self.manifest_dataset_unresolved, ayeaye.Connect):
            # .clone() is to prevent the .Connect being bound to the parent if it's connected
            manifest_dataset = self.manifest_dataset_unresolved.clone()
        else:
            manifest_dataset = self.manifest_dataset_unresolved

        # create ephemeral dataset not tied to an ayeaye.Model
        e_url = ayeaye.connector_resolver.resolve(manifest_dataset.engine_url)
        self._manifest_dataset = ayeaye.Connect(engine_url=e_url)

        yield from self._manifest_dataset.data[self.field_name]

    @property
    def mapper_methods(self):
        """
        mapper methods are those with names starting `map_`.

        The `map_name` is the part afer `map_`.

        For example-

        ```
        def map_xxxx(self):
            ...
        ```

        `xxxx` is the `map_name`.

        @returns (dict) map_name -> mapping_method bound to instance
        """
        method_prefix = 'map_'

        mapper_methods = {}
        for obj_name in dir(self):

            if not obj_name.startswith(method_prefix):
                continue

            obj = getattr(self, obj_name)
            if ismethod(obj):
                map_name = obj_name[len(method_prefix):]

                if map_name == 'manifest_item':
                    raise ValueError("Reserved method name: manifest_item")

                mapper_methods[map_name] = obj

        return mapper_methods

    @property
    def full_map(self):
        """

        manifest_item -> map_name -> [mapped_value[,mapped_value...]]

        """
        # loop through all items in manifest. This ensures at least
        # an empty map_name dictionary if no mapper targets that item.
        full_map = {}
        for manifest_listed_file in self.manifest_items:
            full_map[manifest_listed_file] = defaultdict(list)

        for map_name, map_method in self.mapper_methods.items():

            for manifest_listed_file, engine_url in map_method():
                full_map[manifest_listed_file][map_name].append(engine_url)

        return full_map

    def __iter__(self):
        """
        yield :class:`ayeaye.Pinnate` objects with map_names as the attributes and the additional
        attribute `manifest_item`.
        """
        for manifest_item, map_values in self.full_map.items():
            m = {'manifest_item': manifest_item}
            for map_name, engine_urls in map_values.items():

                # 1-1 mapping easier to understand when returned as single item
                # if this isn't intuitive then map_method() should declare what to do here.
                # e.g.
                # always return list (possibly with one item) when dealing with multi-connector.
                m[map_name] = engine_urls[0] if len(engine_urls) == 1 else engine_urls

            yield ayeaye.Pinnate(m)

    def __getattr__(self, attr):
        """
        map_xxx() method becomes .xxx() method and .xxx() only returns the engine_urls, not the file
        from the manifest. These `map_name` methods are for passing engine_urls to
        :class`ayeaye.Connect` in the class variable dataset declaration part of an
        :class:`ayeaye.Model` where the mapper will be called later.
        """
        if attr not in self.mapper_methods:
            cls_name = self.__class__.__name__
            attrib_error_msg = f"'{cls_name}' object has no attribute '{attr}'"
            raise AttributeError(attrib_error_msg)

        full_map_name_method = self.mapper_methods[attr]

        def engine_url_reducer():
            return [m[1] for m in full_map_name_method()]

        return engine_url_reducer
