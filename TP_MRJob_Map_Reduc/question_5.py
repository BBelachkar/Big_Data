#!/usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep

class VenteMapReduce(MRJob):

    def mapper(self, _, line):
        # Supprimer les espaces inutiles et diviser la ligne en colonnes
        columns = line.strip().split('\t')
        
        # Extraire la ville d'achat et la catégorie d'achat
        city = columns[2]
        category = columns[3]
        
        # Émettre la combinaison (ville, catégorie) comme clé et 1 comme valeur
        yield (city, category), 1

    def reducer_count(self, key, counts):
        # Somme des occurrences pour chaque combinaison (ville, catégorie)
        total_count = sum(counts)
        
        # Émettre le résultat intermédiaire
        yield key, total_count

    def reducer_pivot(self, key, counts):
        # Pivoter les données pour obtenir une table où les colonnes sont les catégories
        # et les lignes sont les villes, avec le nombre d'occurrences comme valeurs
        city, category = key
        total_count = sum(counts)
        yield city, (category, total_count)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_count),
            MRStep(reducer=self.reducer_pivot)
        ]

if __name__ == '__main__':
    VenteMapReduce.run()


