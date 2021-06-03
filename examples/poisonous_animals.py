from collections import defaultdict

import ayeaye


class PoisonousAnimals(ayeaye.Model):
    """
    Super simple ETL.

    Just using normal python data structures, group all the poisonous animals by country where
    they are found.
    """
    poisonous_animals = ayeaye.Connect(engine_url='json://data/poisonous_animals.json')

    def build(self):
        by_country = defaultdict(list)
        for animal in self.poisonous_animals.data.animals:
            by_country[animal.where].append(animal.name)

        # Use log this so we can see it
        for country, animals in by_country.items():
            these_animals = ",".join(animals)
            msg = f"In {country} you could find {these_animals}"
            self.log(msg)


if __name__ == '__main__':
    m = PoisonousAnimals()
    m.go()
