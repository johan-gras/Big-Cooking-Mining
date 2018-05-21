# If MySQLdb not installed

# pip install mysqlclient

import MySQLdb
import pandas as pd

'''
class Recipe(IsDescription):
    id_recipe           = Int32Col()
    title_recipe        = StringCol(itemsize=200)  # 200-character string
    level               = Int32Col()               # integer
    number_of_person    = Int32Col()               # integer
    budget              = Float32Col() #  floats (single-precision)
    rating              = Float64Col() #  doubles (double-precision)

    h5file = open_file("recipes.h5", mode="w", title="Recipes Test File")
    group = h5file.create_group("/", 'detector', 'Detector information')
    #table = h5file.create_table(group, 'readout', Recipe, "Readout example")
'''


class Database:
    # Database arguments :

    location = "localhost"

    user = "root"

    password = ""

    database_name = "bigcookingdata"

    db = None

    cursor = None

    ## Json file Reader
    # recipes = JsonReader()
    file_rec = 'new.json'

    def connect(self):

        self.db = MySQLdb.connect(self.location, self.user, self.password, self.database_name)

        self.cursor = self.db.cursor()

    def disconnect(self):

        self.db.close()

    def get_version(self):

        self.cursor.execute("SELECT VERSION()")

        return self.cursor.fetchone()[0]

    def create_db(self):

        self.connect()

        sql = """
                DROP TABLE IF EXISTS RECIPE;
                DROP TABLE IF EXISTS INGREDIENT;
                DROP TABLE IF EXISTS USER_PROFILE;
                DROP TABLE IF EXISTS UTENSIL;
                DROP TABLE IF EXISTS RECIPE_STEP;
                DROP TABLE IF EXISTS SPORT_ACTIVITY;



                CREATE TABLE RECIPE (
                 ID_RECIPE INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
                 TITLE_RECIPE  VARCHAR(200),
                 LEVEL_RECIPE VARCHAR(100),  
                 NUMBER_OF_PERSON VARCHAR(50),
                 RATING_RECIPE VARCHAR(100),
                 TIME_TOTAL VARCHAR(100),
                 TIME_PREPA VARCHAR(100),
                 TIME_COOKING VARCHAR(100));

                 CREATE TABLE INGREDIENT (
                 ID_INGREDIENT INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                 NAME_INGREDIENT VARCHAR(100));

                 CREATE TABLE USER_PROFILE (
                 ID_USER INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                 USERNAME VARCHAR(50),
                 BIRTHDAY_USER DATETIME,
                 MAIL_USER VARCHAR(100),
                 WEIGHT_USER VARCHAR(50),
                 HEIGHT_USEER VARCHAR(50),
                 REGISTRATION_DATE DATETIME);

                 CREATE TABLE UTENSIL (
                 ID_UTENSIL INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                 NAME_UTENSIL VARCHAR(100),
                 QUANTITY VARCHAR(20));

                 CREATE TABLE RECIPE_STEP (
                 ID_STEP INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                 STPE_NUMBER VARCHAR(50),
                 DESCRIPTION_STEP VARCHAR(255));

                 CREATE TABLE SPORT_ACTIVITY (
                 ID_ACTIVITY INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                 NAME_ACTIVITY VARCHAR(100),
                 ACTIVITY_TYPE VARCHAR(100),
                 FREQUENCY_ACTIVITY VARCHAR(50));"""

        self.cursor.execute(sql)

        self.disconnect()

        print("La Base de données a bien été créée")

    def build_db(self, file_rec):

        compteur = 0
        self.connect()
        liste_ingre_totale = []
        liste_ingre_by_recipe = []

        for values in file_rec.recipes:

            compteur = compteur + 1
            print("Boucle For ", compteur)

            steps = values.get('etapes')
            number_of_person = values.get('number_of_person')
            time_total = values.get('time').get('total')
            time_prepa = values.get('time').get('preparation')
            time_cooking = values.get('time').get('cooking')
            rating = values.get('rating')
            level = values.get('level')
            category = values.get('category')
            ingredients = values.get('ingredients')
            budget = values.get('budget')
            title_recipe = values.get('title')

            print(title_recipe)
            for step in steps:
                step_num = step['Etape']
                step_desc = step['Description']
                # title_r = title_recipe

                sql_step = """insert into recipe_step (step_number,desc_step, title_recipe) values ('%s','%s','%s');""" % (
                step_num, step_desc, title_recipe)
                print(sql_step)

            print(title_recipe)
            for ing in ingredients:
                liste_ingre = []
                liste_ingre_by_recipe = []
                ing_d = ing.get('id')
                ing_quantity = ing.get('quantity')
                ing_name = ing['name']
                liste_ingre.append(ing_name)

                sql_ingredient = """ insert into ingredient (name_ingredient) values('%s','%s','%s');""" % (
                ing_name, ing_quantity, title_recipe)
                # print(ing_quantity)
                print(sql_ingredient)

            liste_ingre_by_recipe.extend(liste_ingre)
            liste_ingre_totale.extend((liste_ingre_by_recipe))
            liste_ingre_totale_pd = pd.Series(liste_ingre_totale).drop_duplicates().tolist()
            # print("Liste ingredient panda sans doublons", len(liste_ingre_totale_pd))

            ######### Injection des données en base

            sql_recipe = """insert into recipe (title,level,number_of_person,rating,time_total,time_prepa,time_cooking)\
            VALUES ('%s','%s','%s','%s','%s','%s','%s');""" % (
            title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking)

            print(sql_recipe)

            try:
                # Execute the SQL command
                # self.cursor.execute(sql_recipe)
                # self.cursor.execute(sql_step)
                # self.cursor.execute(sql_ingredient)
                # Commit your changes in the database
                self.db.commit()
            except:
                # Rollback in case there is any error
                self.db.rollback()

        self.disconnect()

        print("La Base de données a bien été importée")

    def truncate_table_recipe(self):
        self.connect()

        sql = "truncate table recipe;"

        try:
            # Execute the SQL command
            self.cursor.execute(sql)
            # Commit your changes in the database
            self.db.commit()
        except:
            # Rollback in case there is any error
            self.db.rollback()

        self.disconnect()

        print("La table recipe est bien vidée")

    def build_table_ingredient(self, file_rec):

        compteur = 0
        liste_ingre_totale = []
        liste_ingre_by_recipe = []

        self.connect()

        for values in file_rec.recipes:

            liste_ingre = []
            liste_ingre_by_recipe = []
            compteur = compteur + 1
            print("***** Contador recetas : ", compteur, "*****")
            ingredients = values.get('ingredients')
            title = values.get('title')

            for ing in ingredients:
                # print(ing['name'])
                ing_name = ing['name']
                liste_ingre.append(ing_name)

                sql = """ insert into ingredient (name_ingredient) values('%s');""" % (ing_name)
                try:
                    # Execute the SQL command
                    # self.cursor.execute(sql)
                    # Commit your changes in the database
                    self.db.commit()
                except:
                    # Rollback in case there is any error
                    self.db.rollback()

            liste_ingre_by_recipe.extend(liste_ingre)
            print("___Nombre d'ingredients pour la recette", title, ":", len(liste_ingre_by_recipe))
            print("___Ingrédients :", liste_ingre_by_recipe)

            liste_ingre_totale.extend((liste_ingre_by_recipe))
            # [liste_ingre_totale.extend(i) for i in liste_ingre_by_recipe if not i in liste_ingre_totale]
            # print("Liste ingre everseen totale sans doublons",len(list(unique_everseen(liste_ingre_totale))))
            liste_ingre_totale_pd = pd.Series(liste_ingre_totale).drop_duplicates().tolist()
            print("Liste ingre  panda sans doublons", len(liste_ingre_totale_pd))
            print("-----Ingrédients totaux pour toute la base :", liste_ingre_totale_pd, "-----")

        self.disconnect()





