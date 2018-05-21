from JsonReader import JsonReader
from Database_Creator import Database
from Stats import Stats
from Prototype import Prototype
from Clustering import Clustering

close = False
json = JsonReader()
db = Database()
stats = Stats(json)
recommender = Prototype(json)
clustering = Clustering(json)


def user_command():
    global close
    command = input()

    # Id recipe
    if command.isdigit():
        json.id = int(command)
        json.read_recipe()

    # Next recipe
    elif not command:
        json.id += 1
        json.read_recipe()

    # Create/recreate database
    elif command == "create_db":
        db.create_db()

    # Create/recreate database
    elif command == "build_db":
        db.build_db(json)

    # Vider la table recipe
    elif command == "truncate_table_recipe":
        db.truncate_table_recipe()

    elif command == "build_table_ingredient":
        db.build_table_ingredient(json)

    # Appel au module stats
    elif command.find('stats ') != -1:
        command = command.replace('stats ', '')
        getattr(stats, command)()

    elif command.find('stats') != -1:
        stats.helper()

    # Appel au module prototype
    elif command.find('recommender ') != -1:
        command = command.replace('recommender ', '')
        getattr(recommender, command)()

    elif command.find('recommender') != -1:
        recommender.helper()

    # Appel au module cluster
    elif command.find('cluster ') != -1:
        command = command.replace('cluster ', '')
        getattr(clustering, command)()

    # Exit
    elif command == 'exit':
        close = True

    else:
        print("Unknow command !\n")


def main():

    print('Fichier ouvert avec ' + str(len(json.recipes)) + ' recettes\n')
    print('Taper un id de recette')
    print('Appuyer sur entré pour les faire défilé\n')
    print('"create_db" pour créer ou recrée la base')
    print('"build_db" pour remplir la base avec les recettes')
    print('"truncate_table_recipe" pour vider la table recipe')
    print('"build_table_ingredient" pour remplir la base avec les ingredient')
    print('"stats" pour le module de statistique')
    print('"recommender" pour le module de recommendation')
    print('"exit" pour fermé')

    while not close:
        print()
        user_command()


main()
