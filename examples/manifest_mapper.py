"""
Demonstrate a one to one mapping from input file to output file and filtering records.


pipenv shell
export PYTHONPATH=`pwd`
python examples/manifest_mapper.py 
2021-06-23 12:27:50 INFO      All done!
less examples/australian_aquatic_animals.json 


"""
import os

from ayeaye.common_pattern.manifest import AbstractManifestMapper


import ayeaye


class FileMapper(AbstractManifestMapper):

    def map_menagerie(self):
        """The manifest lists just the filename. All the files are in same 'input_data' directory
        as the manifest file. The connector_resolver will fill the template values.
        """
        return [(f, "json://{input_path}/" + f"{f}") for f in self.manifest_items]

    def map_oz_animals(self):
        return [(f, "ndjson://{output_path}/australian_" + f"{f}") for f in self.manifest_items]


class AustralianAnimals(ayeaye.Model):
    """
    Take multiple input json:// datasets listed in a manifest and for each file output a
    corresponding file with just the Australian animals.

    This is a demonstration of :class:`AbstractManifestMapper`.
    """

    animals_manifest = ayeaye.Connect(engine_url="json://{input_path}/animals_manifest.json")
    animals_mapper = FileMapper(animals_manifest, "animal_files")

    menagerie = ayeaye.Connect(engine_url=animals_mapper.menagerie)

    australian_animals = ayeaye.Connect(engine_url=animals_mapper.oz_animals,
                                        access=ayeaye.AccessMode.WRITE)

    def build(self):
        for mapper in self.animals_mapper:
            input_dataset = self.menagerie[mapper.menagerie]
            for animal in input_dataset.data.animals:

                if animal.where == "Australia":
                    # multiple output files with 'australian_' prefixed to input file name
                    self.australian_animals[mapper.oz_animals].add(animal)

        self.log("All done!")


if __name__ == '__main__':
    examples_path = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(examples_path, "data")

    with ayeaye.connector_resolver.context(input_path=input_path, output_path=examples_path):
        m = AustralianAnimals()
        m.go()
