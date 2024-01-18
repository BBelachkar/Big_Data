import re
from tabulate import tabulate
import matplotlib.pyplot as plt

# Ouvrir le fichier en mode lecture
with open('C:\\Users\\MouadKHAZNAOUI\\TP_BigData_ECL\\TP_Hadoop\\wordcount\\result.txt', 'r') as file:
    # Initialiser une liste vide pour stocker les données
    data_list = []

    # Lire chaque ligne du fichier
    for line in file:
        # Utiliser une expression régulière pour extraire les informations nécessaires
        match = re.match(r'"([^"]+)"\s+\["([^"]+)",\s+(\d+)\]', line)
        if match:
            # Extraire les informations correspondantes aux groupes de l'expression régulière
            city = match.group(1)
            category = match.group(2)
            value = int(match.group(3))

            # Ajouter les informations extraites à la liste
            data_list.append([city, category, value])

# Créer des dictionnaires pour stocker les données par catégorie et par ville
data_by_city = {}
data_by_category = {}

for item in data_list:
    city, category, value = item
    if city not in data_by_city:
        data_by_city[city] = []
    data_by_city[city].append((category, value))

    if category not in data_by_category:
        data_by_category[category] = []
    data_by_category[category].append((city, value))

# Afficher le tableau résultant
headers = ["City", "Category", "Value"]
table = tabulate(data_list, headers, tablefmt="pretty")
print(table)

# Créer des graphiques à barres pour chaque ville
for city, data in list(data_by_city.items())[:2]:
    categories, values = zip(*data)
    plt.figure(figsize=(15, 10))
    plt.bar(categories, values)
    plt.xlabel('Category')
    plt.ylabel('Value')
    plt.title(f'{city} - Sales by Category')
    plt.show()

# Créer un graphique à barres pour chaque catégorie
for category, data in list(data_by_category.items())[:2]:
    cities, values = zip(*data)
    plt.figure(figsize=(15, 10))
    plt.bar(cities, values)
    plt.xlabel('City')
    plt.ylabel('Value')
    plt.title(f'{category} - Sales by City')
    plt.show()