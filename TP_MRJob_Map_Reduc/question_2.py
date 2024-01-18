#!/usr/bin/env python3

from mrjob.job import MRJob

class VenteMapReduce(MRJob):

    def mapper(self, _, line):
        # Supprimer les espaces inutiles et diviser la ligne en colonnes
        columns = line.strip().split('\t')
        
        # Extraire la catégorie d'achat et le montant de la vente
        category = columns[3]
        amount = float(columns[4])  # Supposons que le montant est dans la colonne 4
        
        # Émettre la catégorie d'achat et le montant de la vente pour chaque ligne
        yield category, amount

    def reducer(self, category, amounts):
        # Somme des montants pour chaque catégorie
        total_amount = sum(amounts)
        
        # Émettre le résultat final
        yield category, total_amount

if __name__ == '__main__':
    VenteMapReduce.run()
