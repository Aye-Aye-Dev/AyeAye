"""
A manifest is "a list of people and goods carried on a ship or plane" but here, it's a list of
datasources.

A common pattern is to bootstrap a batched build by listing all the target files used in the build in a
manifest file. This manifest file is a dataset for any model that works on the target files. 
"""

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
        if isinstance(manifest_dataset, ayeaye.Connect):
            # .clone() is to prevent the .Connect being bound to the parent if it's connected
            self.manifest_dataset = manifest_dataset.clone()
        else:
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
        # create ephemeral dataset not tied to an ayeaye.Model
        e_url = ayeaye.connector_resolver.resolve(self.manifest_dataset.engine_url)
        ds = ayeaye.Connect(engine_url=e_url)
        dataset_section = ds.data[self.field_name]

        # :class:`connectors.multi_connector.MultiConnector` vs. single engine_urls connectors
        if isinstance(dataset_section, list):
            return [f"{self.engine_type}://{f}" for f in dataset_section]

        return f"{self.engine_type}://{dataset_section}"
