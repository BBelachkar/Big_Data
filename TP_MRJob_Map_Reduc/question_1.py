#!/usr/bin/env python3

from mrjob.job import MRJob

class VenteMapReduce(MRJob):

    def mapper(self, _, line):
        # Supprimer les espaces inutiles et diviser la ligne en colonnes
        columns = line.strip().split('\t')
        
        # Extraire la catégorie d'achat
        category = columns[3]
        
        # Émettre la catégorie d'achat et 1 pour chaque ligne
        yield category, 1

    def reducer(self, category, counts):
        # Somme des occurrences pour chaque catégorie
        total_count = sum(counts)
        
        # Émettre le résultat final
        yield category, total_count

if __name__ == '__main__':
    VenteMapReduce.run()

