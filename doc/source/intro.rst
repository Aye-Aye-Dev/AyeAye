.. _intro:

A Brief Introduction
====================

ETL
---

The best layout for data depends on how the data is being used. For example, the dataset of readings collected from a temperature sensor would be organised in the order the readings were taken and the storage would be quick to append readings to. The analysis of the same data would be organised around gaining insight so it should be easy to ask questions of the data- for example: At what time was the maximum temperature?

Sometimes a single datastore can work for both cases. The rest of the time the data needs to be transformed from the operational (readings) dataset to the analytic dataset.

ETL (Extract Transform Load) is the process of reshaping data from one form to another. There are loads of ways to do this.


Design goals for Aye-aye
------------------------

A helpful framework, not rails
    Aye-aye mustn't interfere with developers using the power and flexibility of normal Python. It should be possible to opt into features of Aye-aye and Aye-aye mustn't mandate a particular formula or way of working. Best practice will only be communicated through the documentation and not through API controls and restrictions. 

Same code in discovery, dev, unittests and production
    During the early (discovery) stages of a project the code in part used to investigate the problem space. In some environments this code is passed onto data engineers to 'production-ise' which results in a codebase that isn't usable to re-investigate the assumptions built during discovery. In some ML spaces the models break down and re-building the discovery process requires starting from scratch as production code no longer runs in a Jupyter notebook for example. Aye-aye must support a way of working that allows the same code to be run in a variety of environments.    

Break up complex models
    Getting the scope right is critical to success in every aspect of engineering. In software engineering each module should have a `single responsibility <https://en.wikipedia.org/wiki/Single-responsibility_principle>`_ so Aye-aye should provide a way to split a large model into smaller more manageable pieces without creating a complex task of running the sub-tasks and joining sub-results back together.

Scale easily
    The transition from a single process ETL to a partitioned model that runs sub-tasks in parallel shouldn't have more than a modest increase in coding or debugging complexity.

Inspect data flow and data provenance
    It should be straightforward to inspect or visualise the data provenance (also known as data lineage) of datasets. Aye-aye should provide help or tooling to do this the dataset level.

Repeatability
    It should be possible for the re-run an ETL process to result in semantically equivalent output datasets. The software engineering effort to do this should be reduced by tooling provided by Aye-aye.

Connect islands of data
    The source data for one ETL process is often the output from another ETL process. Aye-aye should provide a way to keep the independence of each ETL process/chain. This is to prevent monolithic code and lock-in type dependencies between separate code bases. e.g. departments in a company.

Smoother learning curve
    The learning curve from beginner to more advanced techniques should be intuative.

Overall complexity
    The complexity of an ETL system should be proportional to the complexity of the task. There are a lot of ETL methodologies out there that require a mastery of too many skills. You shouldn't need experience in AWS, devops, Linux system admin etc. just to combine a few CSV files together!

