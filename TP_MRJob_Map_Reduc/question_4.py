#!/usr/bin/env python3

from mrjob.job import MRJob

class VenteMapReduce(MRJob):

    def mapper(self, _, line):
        # Supprimer les espaces inutiles et diviser la ligne en colonnes
        columns = line.strip().split('\t')
        
        # Extraire la catégorie d'achat, la ville, la somme dépensée et le moyen de paiement
        category = columns[3]
        city = columns[2]
        amount_spent = float(columns[4])
        payment_method = columns[5]
        
        # Émettre la combinaison (catégorie, ville, paiement) et la somme dépensée pour chaque ligne
        yield (category, city, payment_method), amount_spent

    def reducer(self, key, amounts):
        # Somme des montants dépensés pour chaque combinaison (catégorie, ville, paiement)
        total_amount = sum(amounts)
        
        # Émettre le résultat final uniquement si la catégorie est Women's Clothing et le moyen de paiement est Cash
        if key[0] == "Women's Clothing" and key[2] == "Cash":
            yield key[1], total_amount

if __name__ == '__main__':
    VenteMapReduce.run()
