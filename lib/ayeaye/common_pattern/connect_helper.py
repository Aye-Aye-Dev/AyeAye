class MultiConnectorNewDataset:
    """
    Multi-connectors are often used to produce a number of similar files. This helper class makes
    an additional method on a :class:`MultiConnector` connection to make it easy to create files
    using a templated engine_url.

    This is done using the `method_overlay` argument to the :class:`DataConnector` superclass. It
    can be used like this-

    # (as a class variable)
    output_file_template = "csv://{product_group}_{product_name}_parts_list.csv"
    components_doc = ayeaye.Connect(
        engine_url=[],
        method_overlay=(MultiConnectorNewDataset(template=output_file_template), "new_dataset"),
        access=ayeaye.AccessMode.WRITE,
    )
    ...
    # then use it like this ...
    components = self.components_doc.new_dataset(product_group="machinery", product_name="digger")

    # components is now a new CSV data connector, add something to it
    components.add({"name": "spring", "product_code": "ab123"})

    Also, see unittest :class:`TestConnectHelper`.
    """

    def __init__(self, template):
        """
        @param template: (str)
            template for an engine_url
        """
        self.template = template

    def __call__(self, parent_connector, *args, **kwargs):
        """
        @param kwargs: dict with strings for both key and value
            These will be substituted into `self.template`
        """
        resolved_template = self.template
        for k, v in kwargs.items():
            template_field = "{" + k + "}"
            if template_field in self.template:
                # can't use .format as not all template fields are being replaced
                resolved_template = resolved_template.replace(template_field, v)

        new_dataset_connection = parent_connector.add_engine_url(resolved_template)
        return new_dataset_connection
