from JsonReader import JsonReader

close = False
json = JsonReader()


def user_command():
    global close
    command = input()

    if command.isdigit():
        json.id = int(command)
        json.read_recipe()
    elif not command:
        json.id += 1
        json.read_recipe()
    elif command == 'exit':
        close = True


def main():
    print('Fichier ouvert avec ' + str(len(json.recipes)) + ' recettes\n')
    print('Taper un id de recette')
    print('Taper entré pour les faire défilé')
    print('Fermé avec exit')

    while not close:
        print()
        user_command()


main()
