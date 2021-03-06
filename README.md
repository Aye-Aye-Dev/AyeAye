# Aye Aye

An ETL (Extract, Transform, Load) framework.

## Quick start

In the project you’d like to use Aye Aye in, run:-

```shell
pipenv shell
pipenv install -e git+https://github.com/Aye-Aye-Dev/AyeAye#egg=ayeaye
```

Within the environment created by pipenv above, run one of the examples:-

```shell
ln -s `pipenv --venv`/src/ayeaye/examples/
cd examples
python poisonous_animals.py 
```

This model takes a small input dataset of animals and collates them by the country they are found. It doesn't write to a dataset, it just outputs a log. The log for this example contains the name of the country and the animals found there.

The `ln` (symlink) command will make all the modules in the examples directory visible. You could just change into this directory with the command:-

```shell
cd `pipenv --venv`/src/ayeaye/examples
```

There are more examples in the [Aye-Aye-Recipes](https://github.com/Aye-Aye-Dev/Aye-Aye-Recipes) git repo.


## Overview

An Aye Aye ETL *model* inherits from `ayeaye.model` and uses class level variables to declare *connectors* to the data it acts on.

Example:-

```python
import ayeaye

class PoisonousAnimals(ayeaye.Model):
    poisonous_animals = ayeaye.Connect(engine_url='ndjson://data/poisonous_animals.ndjson')
```

When instantiated, `self.poisonous_animals` will be a *dataset* that ETL operations can be done with.

The `engine_url` parameter passed to `ayeaye.Connect` is specifying the dataset type ([new line JSON](http://ndjson.org/) in this case) and exact location for the data (`data/poisonous_animals.flowerpot` is a relative file path).

Instead of `engine_url` you could also specify a `ref` and this uses the data catalogue to lookup the `engine_url`. (TODO this feature is coming soon!). When used this way, `ayeaye.Connect` is responsible for resolving the `ref` to an `engine_url` and passing this to a subclass of `ayeaye.connectors.base.DataConnector` which can read and maybe write this data type.


## Unit tests

Ensure the working directory is the base Aye Aye directory (i.e. the same directory as the Pipfile):
```shell
pipenv install --dev
pipenv run python -m unittest tests/test_*.py
```


## License

Aye Aye is distributed under the terms of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html) and Copyright Progressive Logic Limit 2020 and onwards.
