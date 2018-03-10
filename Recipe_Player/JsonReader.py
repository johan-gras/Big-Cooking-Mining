import json





class JsonReader:

    file = 'official.json'

    id = -1



    def __init__(self):

        json_file = open(self.file, encoding="utf8")

        json_str = json_file.read()

        self.recipes = json.loads(json_str)

        json_file.close()



    # Safe getter for access to dictionary attributes

    def get_att(self, attribut, dict):
        if dict == 'none':

            return 'none'

        return dict.get(attribut, 'none')



    def read_recipe(self):

        r = self.recipes[self.id]



        print('Id : ' + str(self.id))

        print(self.get_att('title', r) + '    (' + self.get_att('url', r) + ')\n')

        print(

            'Personnes ' + self.get_att('number_of_person', r) + ' / Temps ' + self.get_att('preparation', self.get_att('time', r))

            + ' - ' + self.get_att('cooking', self.get_att('time', r)) + ' - ' + self.get_att('total',

                                                                                  self.get_att('time', r)) + ' / Level '

            + self.get_att('level', r) + ' / Budget ' + self.get_att('budget', r) + ' / Rating ' + self.get_att('rating', r) + '\n')



        print('Ingredients : ', end='')

        for ingredient in r.get('ingredients', []):

            print(self.get_att('quantity', ingredient) + ' ' + self.get_att('name', ingredient) + '    ', end='')



        print('\nUstensils : ', end='')

        for utensil in r.get('utensils', []):

            print(utensil + '    ', end='')



        print('\n')

        for etape in r.get('etapes', []):

            print(str(self.get_att('Etape', etape)) + ' ' + self.get_att('Description', etape))