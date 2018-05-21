# Recette_Scrap

2 modules sont disponible : Recette_Scrap et Recipe_Player

Recette_Scrap réalise le scraping des recettes avec le framework Scrapy.
cd repertoire_du_projet
scrapy crawl scrap_sitemap -o test.json

Recipe_Player est un script permettant l'éxécution de commandes interactive sur des recettes :
Lecture de recettes, création et remplissage de BDD.
Mettre dans Recette_Player/ un fichier de recette 'official.json'.
Exécuté le script Recipe_Player.py
