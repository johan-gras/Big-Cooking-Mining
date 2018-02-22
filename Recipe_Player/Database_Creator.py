# If MySQLdb not installed
# pip install mysqlclient

import MySQLdb
import pandas as pd
#from more_itertools import unique_everseen

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

    database_name = "big_cooking_data"

    db = None

    cursor = None


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
                DROP TABLE IF EXISTS STEP;
                DROP TABLE IF EXISTS SPORT_ACTIVITY;
                DROP TABLE IF EXISTS CATEGORIE;
                DROP TABLE IF EXISTS L_RECIPE_STEP;
                DROP TABLE IF EXISTS L_RECIPE_INGREDIENT;

                CREATE TABLE RECIPE (
                 ID_RECIPE INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
                 TITLE_RECIPE VARCHAR(200),
                 LEVEL_RECIPE VARCHAR(100),  
                 NUMBER_OF_PERSON VARCHAR(50),
                 RATING_RECIPE VARCHAR(100),
                 TIME_TOTAL VARCHAR(100),
                 TIME_PREPA VARCHAR(100),
                 TIME_COOKING VARCHAR(100),
                 BUDGET VARCHAR(20));

                 CREATE TABLE INGREDIENT (
                 ID_INGREDIENT INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                 NAME_INGREDIENT VARCHAR(100),
                 QUANTITY_INGREDIENT VARCHAR(20),
                 TITLE_RECIPE VARCHAR(255),
                 ING_ID VARCHAR(20));

                 CREATE TABLE USER_PROFILE (
                 ID_USER INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                 USERNAME VARCHAR(50),
                 BIRTHDAY_USER DATETIME,
                 MAIL_USER VARCHAR(100),
                 WEIGHT_USER VARCHAR(50),
                 HEIGHT_USEER VARCHAR(50),
                 REGISTRATION_DATE DATETIME);

                 CREATE TABLE UTENSIL (
                 NAME_UTENSIL VARCHAR(100),
                 TITLE_RECIPE VARCHAR(255));

                 CREATE TABLE STEP (
                 ID_STEP INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                 STEP_NUMBER VARCHAR(50),
                 DESCRIPTION_STEP VARCHAR(255));
                 
                 CREATE TABLE CATEGORIE (
                 ID_CATEGORIE INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                 CATEGORIE_NAME VARCHAR(255),
                 TITLE_RECIPE VARCHAR(255));
                 
                 CREATE TABLE L_RECIPE_STEP (
                 TITLE_RECIPE VARCHAR(255),
                 DESCRIPTION_STEP VARCHAR(255));
                 
                 CREATE TABLE L_RECIPE_INGREDIENT (
                 TITLE_RECIPE VARCHAR(255),
                 NAME_INGREDIENT VARCHAR(255));

                 CREATE TABLE SPORT_ACTIVITY (
                 ID_ACTIVITY INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                 NAME_ACTIVITY VARCHAR(100),
                 ACTIVITY_TYPE VARCHAR(100),
                 FREQUENCY_ACTIVITY VARCHAR(50));"""

        self.cursor.execute(sql)

        self.disconnect()

        print("La Base de données a bien été créée")

    def build_db(self, recipes):

        global sql_recipe, sql_step, sql_ingredient, sql_utensils, liste_ingre_totale_pd
        compteur = 0
        liste_ingre_totale = []
        liste_ingre_by_recipe = []
        self.connect()
        for values in recipes.recipes:

            steps = values.get('etapes')
            number_of_person = values.get('number_of_person')
            time = values.get('time')
            rating = values.get('rating')
            level = values.get('level')
            categories = values.get('categories')
            ingredients = values.get('ingredients')
            budget = values.get('budget')
            title_recipe = values.get('title')
            utensils = values.get('utensils')
            time_total = 0
            time_prepa = 0
            time_cooking = 0

            #Condition if None everywhere
            if title_recipe and level and number_of_person and rating and budget and steps and time_total and time_prepa and time_cooking is None:
                pass
            # Affichage numerous recipe et compteur
            compteur = compteur + 1
            print("--- NUMEROUS RECIPE :", compteur, ":", title_recipe, "---")

            # Table Recipe
            # Condition if Budget is None
            if budget is None:
                budget = 0
                sql_recipe = """insert into recipe (title_recipe,level,number_of_person,rating,time_total,time_prepa,time_cooking,budget)\
                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget, categories)
            # Condition if Time is None
            elif time is None:
                sql_recipe = """insert into recipe (title_recipe,level,number_of_person,rating,time_total,time_prepa,time_cooking,budget)\
                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                title_recipe, level, number_of_person, rating, 0, 0, 0, budget, categories)

            # Time == full
            else:
                time_total = values.get('time').get('total')
                time_prepa = values.get('time').get('preparation')
                time_cooking = values.get('time').get('cooking')
                sql_recipe = """insert into recipe (title_recipe,level,number_of_person,rating,time_total,time_prepa,time_cooking,budget)\
                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget, categories)
            print("RECIPE ::", sql_recipe)

            # Table step
            if steps is None:
                pass
                print("Pas d'étape")
            else:
                for step in steps:
                    sql_step = ""
                    step_num = step['Etape']
                    step_desc = step['Description']
                    sql_step = """insert into step (step_number,desc_step, title_recipe) values ('%s','%s','%s');""" % (
                    step_num, step_desc, title_recipe)
                    print("STEP ::", sql_step)

            # Table ingredient
            if ingredients is None:
                pass
                print("0 ingrédients")
            else:
                for ing in ingredients:
                    liste_ingre = []
                    liste_ingre_by_recipe = []
                    ing_id = ing.get('id')
                    ing_quantity = ing.get('quantity')
                    ing_name = ing.get('name')
                    liste_ingre.append(ing_name)

                    if ing_quantity is None:
                        sql_ingredient = """ insert into ingredient (name_ingredient,quantity_ingredient,title_recipe,ing_id) values('%s','%s','%s','%s');""" % (
                        ing_name, None, title_recipe, ing_id)
                    else:
                        sql_ingredient = """ insert into ingredient (name_ingredient,quantity_ingredient,title_recipe,ing_id) values('%s','%s','%s','%s');""" % (
                        ing_name, ing_quantity, title_recipe, ing_id)
                    # import ipdb;ipdb.set_trace()
                    print("INGREDIENT :", sql_ingredient)

                    liste_ingre_by_recipe.extend(liste_ingre)
                liste_ingre_totale.extend((liste_ingre_by_recipe))
                liste_ingre_totale_pd = pd.Series(liste_ingre_totale).drop_duplicates().tolist()

            # Table Utensils
            if utensils is None:
                pass
                print("0 utensils")
            else:
                for utensil in utensils:
                    sql_utensils = """ insert into utensil (name_utensil,title_recipe) values ('%s','%s');""" % (
                    utensil, title_recipe)
                    print("UTENSIL ::", sql_utensils)

            # Injection des données en base
            try:
                # Execute the SQL command
                self.cursor.execute(sql_recipe)
                # self.cursor.execute(sql_step)
                # self.cursor.execute(sql_utensils)
                # self.cursor.execute(sql_ingredient)
                # Commit your changes in the database
                self.db.commit()
            except:
                # Rollback in case there is any error
                self.db.rollback()

        self.disconnect()

        print("Nombre d'ingrédients : ", len(liste_ingre_totale_pd))
        print("La Base de données a bien été importée")

    def truncate_db(self):
        self.connect()

        sql_recipe = "truncate table recipe;"
        sql_utensil = "truncate table utensil;"
        sql_ingre = "truncate table ingredient;"
        sql_step = "truncate table step;"
        sql_ingredient = "truncate table ingredient;"
        sql_categorie = "truncate table categorie;"
        sql_l_recipe_step = "truncate table l_recipe_step;"
        sql_l_recipe_ingredient = "truncate table l_recipe_ingredient;"
        sql_sport_activity = "truncate table sport_activity;"
        try:
            # Execute the SQL command
            self.cursor.execute(sql_recipe)
            self.cursor.execute(sql_utensil)
            self.cursor.execute(sql_ingre)
            self.cursor.execute(sql_step)
            self.cursor.execute(sql_ingredient)
            self.cursor.execute(sql_categorie)
            self.cursor.execute(sql_l_recipe_step)
            self.cursor.execute(sql_l_recipe_ingredient)
            self.cursor.execute(sql_sport_activity)

            # Commit your changes in the database
            self.db.commit()
        except:
            # Rollback in case there is any error
            self.db.rollback()

        self.disconnect()

        print("Database bcd is empty")