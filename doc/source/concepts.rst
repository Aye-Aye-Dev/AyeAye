.. _concepts:

Concepts
========

An ETL model is a piece of software that converts data from one form to another. It might filter, convert and or move the data. Here is that idea in a simple diagram-

.. figure:: /_images/concepts_single_model.svg
    :align: center
    :width: 65%

The diagram shows how an Aye-aye model could have one or more datasets as inputs and one or more as outputs. The following incomplete code snippet shows how the dataset connections would be written in an Aye-aye model. This example model has one input and one output.

.. code-block:: python

    import ayeaye

    class FavouriteColours(ayeaye.Model):
        "Summarise people's favorite colours"
    
        favourite_colours = ayeaye.Connect(
            engine_url="csv://data/favourite_colours.csv",
            access=ayeaye.AccessMode.READ,
        )
    
        favourites_summary = ayeaye.Connect(
            engine_url="json://data/favourite_colours_summary.json;indent=4",
            access=ayeaye.AccessMode.WRITE,
        )
    
        def build(self):
            # Todo
            pass


All models should have a single responsibility, for example to extract specific fields or filter out certain records. By keeping the task simple it makes the code easier to reason with and maintain. But you're not going to do anything complex with a single responsibility model so there needs to be a way to composite simple models into a larger model-


.. figure:: /_images/concepts_multi_model.svg
    :align: center
    :width: 65%

This diagram shows three input datasets (A, B and C), three models (X, Y and Z) producing three output dataset (D, E and F). Dataset F probably the main output dataset as it's a leaf node and not used within the model. Datasets D and E might just be internal datasets.

From the Python example above, the `favourites_summary` dataset could be the input to another model-

.. code-block:: python

    class PaintOrder(ayeaye.Model):
        "Buy more popular paint colours"
    
        paint_in_stock = ayeaye.Connect(engine_url="json://data/current_stock_levels.json")
    
        favorite_colours = FavouriteColours.favourites_summary.clone(access=ayeaye.AccessMode.WRITE)
    
        paint_order = ayeaye.Connect(
            engine_url="json://data/paint_order.json",
            access=ayeaye.AccessMode.WRITE,
        )
    
        def build(self):
            # Todo
            pass

The `favourites_summary` dataset in the `FavouriteColours` model is a class variable and can be imported into the `PaintOrder` model. The `.clone()` method changes it into a readonly node without effecting the dataset's useage in the original `FavouriteColours` model.

Importing class variables is a convenient way to connect models together by their inputs and outputs.


Data provenance and build order
===============================

The dataset connections between models form a graph. In the diagram above the graph can be used to find the provenance (i.e. the data origin) of output datasets. Models which have shaped the output data can be found by tracing back in the graph.

Another use of the dataset+models graph is to determine the order to run the models and even which models can be run in parallel. For example, in the diagram above models X and Y could be run in parallel as there isn't a data dependency from one to the other. But both must complete before model Z can be run as it depends on the output from X and Y. The build order in a model is sometimes called the DAG (Directed Acyclic Graph) but that can be confusing as there are DAGS other than the build order.


Repeatability
=============

If the code in the models is deterministic then recording versioning information for the input datasets (i.e. the input leaf nodes of the dataset+models graph) can be used to repeat a build in the future. Aye-aye's terminology for this is `locking`. It's the process of recording the version of the software stack and the data used for a model run.
 