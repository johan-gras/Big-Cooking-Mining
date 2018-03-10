

from JsonReader import JsonReader
from Database_Creator import Database




close = False

json = JsonReader()

db = Database()





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

    elif command == "truncate_db":

        db.truncate_db()



    # Exit

    elif command == 'exit':

        close = True



    else:

        print("Unknow command !\n")





def main():

    print('Fichier ouvert avec ' + str(len(json.recipes)) + ' recettes\n')

    print('Taper un id de recette')

    print('Appuyer sur entré pour les faire défilé')

    print('"create_db" pour créer ou recrée la base')

    print('"build_db" pour remplir la base avec les recettes')

    print('"truncate_db" pour supprimer la table recipe')

    print('"exit" pour fermé')



    while not close:

        print()

        user_command()





main()