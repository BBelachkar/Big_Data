#!/usr/bin/env python3

from mrjob.job import MRJob

class VenteMapReduce(MRJob):

    def mapper(self, _, line):
        # Supprimer les espaces inutiles et diviser la ligne en colonnes
        columns = line.strip().split('\t')
        
        # Extraire la ville d'achat, le montant de la vente, et le moyen de paiement
        city = columns[2]
        amount = float(columns[4])  # Supposons que le montant est dans la colonne 4
        payment_method = columns[5]
        
        # Émettre la ville d'achat comme clé et un tuple (montant, moyen de paiement) comme valeur
        yield city, (amount, payment_method)

    def reducer(self, city, amounts):
        # Initialiser un dictionnaire pour stocker la somme dépensée par chaque moyen de paiement
        total_amounts = {}
        
        # Agréger les montants pour chaque moyen de paiement
        for amount, payment_method in amounts:
            if payment_method not in total_amounts:
                total_amounts[payment_method] = 0
            total_amounts[payment_method] += amount
        
        # Émettre le résultat final
        yield city, total_amounts

if __name__ == '__main__':
    VenteMapReduce.run()
