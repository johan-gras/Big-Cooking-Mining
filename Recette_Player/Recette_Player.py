import json

file = 'test.json'
id_actu = -1
close = False

def open_json(file):
    json_file = open(file)
    json_str = json_file.read()
    return json.loads(json_str)

def get_attr(a, r):
    if r == 'none':
        return 'none'
    return r.get(a, 'none')

def read_receipe(r_id):
    global receipes
    r = receipes[r_id]
    
    print('Id : ' + str(r_id))
    print(get_attr('title', r) + ' (' + get_attr('url', r) + ')\n')
    print('Personnes ' + get_attr('number_of_person', r) + ' / Temps ' + get_attr('preparation', get_attr('time', r))
          + ' - ' + get_attr('cooking', get_attr('time', r)) + ' - ' + get_attr('total', get_attr('time', r)) + ' / Level '
          + get_attr('level', r) + ' / Budget ' + get_attr('budget', r) + ' / Rating ' + get_attr('rating', r) + '\n')
    
    print('Ingredients : ', end='')
    for ingredient in r.get('ingredients', []):
        print(get_attr('quantity', ingredient) + ' ' + get_attr('name', ingredient) + '    ', end='')
    
    print('\nUstensils : ', end='')
    for utensil in r.get('utensils', []):
        print(utensil  + '    ', end='')
    
    print('\n')
    for etape in r.get('etapes', []):
        print(str(get_attr('Etape', etape)) + ' ' + get_attr('Description', etape))

def user_command():
    global id_actu
    global close
    command = input()
    
    if command.isdigit():
        id_actu = int(command)
        read_receipe(id_actu)
    elif not command:
        id_actu += 1
        read_receipe(id_actu)
    elif command == 'exit':
        close = True


receipes = open_json(file)
print ('Fichier ouvert avec ' + str(len(receipes)) + ' recettes\n')
print ('Taper un id de recette')
print ('Taper entré pour les faire défilé')
print ('Fermé avec exit')

while not close:
    print()
    user_command()
