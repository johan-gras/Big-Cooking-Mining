# If MySQLdb not installed


# pip install mysqlclient

import MySQLdb

import time

import pandas as pd

import re

# from more_itertools import unique_everseen

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

    def connect(self):

        self.db = MySQLdb.connect( self.location, self.user, self.password, self.database_name )

        self.cursor = self.db.cursor()

    def disconnect(self):

        self.db.close()

    def get_version(self):

        self.cursor.execute( "SELECT VERSION()" )

        return self.cursor.fetchone()[0]

#######Création base de données

    def create_db(self):

        self.connect()

        sql_create_db = """    
                        DROP TABLE IF EXISTS L_RECIPE_INGREDIENT;
                        DROP TABLE IF EXISTS L_RECIPE_UTENSIL;
                        DROP TABLE IF EXISTS STEP;
                        DROP TABLE IF EXISTS RECIPE;
                        DROP TABLE IF EXISTS INGREDIENT;
                        DROP TABLE IF EXISTS USER_PROFILE;
                        DROP TABLE IF EXISTS UTENSIL; 
                        DROP TABLE IF EXISTS CATEGORIE;

                        CREATE TABLE INGREDIENT (
                        ID_INGREDIENT INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                        NAME_INGREDIENT VARCHAR(100),
                        ING_ID FLOAT);

                        CREATE TABLE UTENSIL (
                        ID_UTENSIL INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                        NAME_UTENSIL VARCHAR(100),
                        TITLE_RECIPE VARCHAR(255));

                        CREATE TABLE RECIPE (
                        ID_RECIPE INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
                        TITLE_RECIPE VARCHAR(200),
                        LEVEL_RECIPE FLOAT,  
                        NUMBER_OF_PERSON FLOAT,
                        RATING_RECIPE FLOAT,
                        TIME_TOTAL VARCHAR(100),
                        TIME_PREPA VARCHAR(100),
                        TIME_COOKING VARCHAR(100),
                        BUDGET FLOAT,
                        CATEGORIES VARCHAR(255));

                        CREATE TABLE L_RECIPE_INGREDIENT (
                        ID_RECIPE INT NOT NULL,
                        ID_INGREDIENT INT NOT NULL,
                        TITLE_RECIPE VARCHAR(255),
                        NAME_INGREDIENT VARCHAR(255),
                        QUANTITY_INGREDIENT FLOAT,
                        CONSTRAINT cst_r_i
                            FOREIGN KEY(ID_RECIPE) REFERENCES RECIPE(ID_RECIPE),
                            FOREIGN KEY (ID_INGREDIENT) REFERENCES INGREDIENT(ID_INGREDIENT));

                        CREATE TABLE L_RECIPE_UTENSIL (
                        ID_RECIPE INT NOT NULL,
                        ID_UTENSIL INT NOT NULL,
                        TITLE_RECIPE VARCHAR(255),
                        NAME_UTENSIL VARCHAR(255),
                        CONSTRAINT cst_r_u
                            FOREIGN KEY (ID_RECIPE) REFERENCES RECIPE(ID_RECIPE),
                            FOREIGN KEY (ID_UTENSIL) REFERENCES UTENSIL(ID_UTENSIL));

                        CREATE TABLE USER_PROFILE (
                        ID_USER INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                        USERNAME VARCHAR(50),
                        BIRTHDAY_USER DATETIME,
                        MAIL_USER VARCHAR(100),
                        WEIGHT_USER FLOAT,
                        HEIGHT_USEER FLOAT,
                        REGISTRATION_DATE DATETIME);

                        CREATE TABLE STEP (
                        ID_STEP INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                        ID_RECIPE INT,
                        TITLE_RECIPE VARCHAR(255),
                        STEP_NUMBER FLOAT,
                        DESCRIPTION_STEP VARCHAR(255),
                        CONSTRAINT cst_s
                            FOREIGN KEY (ID_RECIPE) REFERENCES RECIPE(ID_RECIPE));

                        CREATE TABLE CATEGORIE (
                        ID_CATEGORIE INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                        CATEGORIE_NAME VARCHAR(255),
                        TITLE_RECIPE VARCHAR(255));"""

        sql_t_l_recipe_ingredient = "drop table l_recipe_ingredient;"
        sql_t_l_recipe_utensil = "drop table l_recipe_utensil;"
        sql_step = "drop table step;"
        sql_recipe = "drop table recipe;"
        sql_utensil = "drop table utensil;"
        sql_ingredient = "drop table ingredient;"
        sql_categorie = "drop table categorie;"
        sql_sport_activity = "drop table sport_activity;"
        sql_user_profil = "drop table user_profile;"
        table_l_r_ingredient = 'l_recipe_ingredient'
        table_l_r_utensil = 'l_recipe_utensil'
        table_l_s = 'step'

        _SQL = """SHOW TABLES"""
        self.cursor.execute( _SQL )
        results = self.cursor.fetchall()
        print( 'All existing tables:', results )  # Returned as a list of tuples
        results_list = [item[0] for item in results]  # Conversion to list of str

        if table_l_r_ingredient and table_l_r_utensil and table_l_s in results_list:
            print( table_l_r_ingredient,table_l_r_utensil,table_l_s ,'was found!' )

            try:
                # Execute the SQL command
                self.cursor.execute(sql_t_l_recipe_ingredient)
                self.cursor.execute(sql_t_l_recipe_utensil)
                self.cursor.execute(sql_step)
                self.cursor.execute(sql_recipe)
                self.cursor.execute(sql_ingredient)
                self.cursor.execute(sql_user_profil)
                self.cursor.execute(sql_utensil)
                self.cursor.execute(sql_categorie)
                self.cursor.execute(sql_create_db)
                # Commit your changes in the database
                self.db.commit()

            except:
                # Rollback in case there is any error
                self.db.rollback()


        else:
            print( table_l_r_ingredient,table_l_r_utensil,table_l_s, 'was NOT found!' )
            try:
                # self.cursor.execute(sql_drop)
                self.cursor.execute( sql_create_db )
            except:
                self.db.rollback()

        self.disconnect()

        print( "La Base de données a bien été créée" )



### BUILD_DB


    def build_db(self, recipes):

        global sql_recipe, sql_step, sql_ingredient, sql_utensils, liste_ingre_totale_pd, categories, categorie_str, liste_ingre \
            , utensils, sql_l_recipe_ingredient, dict_ingre, ing_id_unique, ing_quantity, dict_utensil, list_utensil_total_pd, utensil_id_unique, utensil, title_recipe, list_utensil_by_recipe, ingre, id_fetch, id_uten_fetch, \
            id_fetch_rec
        compteur = 0

        liste_ingre_totale = []
        list_utensil_total = []
        dict_ingre = {}

        self.connect()

        ################# Création table ingredients et utensils

        for values in recipes.recipes:

            ingredients = values.get( 'ingredients' )
            categories = values.get( 'categories' )
            title = values.get( 'title' )
            title_rec_tmp = ''.join(str(title))
            title_recipe = re.escape(str(title_rec_tmp))
            utensils = values.get( 'utensils' )
            compteur += 1

            print( "--- NUMEROUS RECIPE :", compteur, ":", title_recipe, "---" )

            # Creation d'un dictionnaire ingrédients sans doublons
            if ingredients is None:
                pass
                print( "0 ingrédients" )
            else:
                print("Nombre d'ingredient :",len(ingredients))

                for ing in ingredients:
                    liste_ingre = []
                    liste_ingre_by_recipe = []
                    ing_id_unique = 0
                    ing_quantity = ing.get( 'quantity' )
                    ing_name = re.escape( str( ing.get( 'name' ) ) )
                    liste_ingre.append( ing_name )
                    liste_ingre_by_recipe.extend( liste_ingre )
                    liste_ingre_totale.extend( liste_ingre_by_recipe )
                    liste_ingre_totale_pd = pd.Series( liste_ingre_totale ).drop_duplicates().tolist()


            # Création d'un dictionnaire utensils sans doublons
            if utensils is None:
                pass
                print( "0 utensils" )
            else:
                print( "Nombre d'utensils :", len( utensils ) )
                print( "Ustensils :", utensils )

                for utensil in utensils:
                    utensil_id_unique = 1
                    list_utensil_total.extend( utensils )
                    list_utensil_total_pd = pd.Series( list_utensil_total ).drop_duplicates().tolist()

            if compteur == 5000:
                break

        print( "LEN LIST INGREDIENT :",len( liste_ingre_totale_pd ) )
        print("LEN LISTE UTENSIL :", len(list_utensil_total_pd))

        # Construction table utensils
        utensil_id_unique = 1
        for uten_thing in list_utensil_total_pd:
            dict_utensil = {uten_thing: utensil_id_unique}
            sql_utensils = """ insert into utensil (name_utensil,title_recipe) values ('%s','%s');""" % (
                uten_thing, utensil_id_unique)
            print( "UTENSIL :", sql_utensils )
            utensil_id_unique += 1

            try:
                # Execute the SQL command
                self.cursor.execute( sql_utensils )
                # Commit your changes in the database
                self.db.commit()
            except:
                # Rollback in case there is any error
                self.db.rollback()

        # Construction table ingredient
        for ingre in liste_ingre_totale_pd:
            dict_ingre = {ingre: ing_id_unique}
            ing_id_unique += 1
            sql_ingredient = """ insert into ingredient (name_ingredient,ing_id) values('%s','%s');""" % (ingre, ing_id_unique)
            print( "INGREDIENT :", sql_ingredient )
            print(len(liste_ingre_totale_pd))

            try:
                # Execute the SQL command
                self.cursor.execute( sql_ingredient )
                # Commit your changes in the database
                self.db.commit()
            except:
                # Rollback in case there is any error
                self.db.rollback()


        #############
        #
        # Création table recipe
        #
        #############

        compteur = 0

        for values in recipes.recipes:

            number_of_person = values.get( 'number_of_person' )
            time = values.get( 'time' )
            rating = values.get( 'rating' )
            level = values.get( 'level' )
            categories = values.get( 'categories' )
            budget = values.get( 'budget' )
            title_recipe = values.get( 'title' )
            utensils = values.get( 'utensils' )
            categorie_tmp = ''.join( str(categories) )
            categorie_str = re.escape((str( categorie_tmp )))
            time_total = 0
            time_prepa = 0
            time_cooking = 0


            # Affichage numerous recipe et compteur
            compteur = compteur + 1
            print( "--- NUMEROUS RECIPE :", compteur, ":", title_recipe, "---" )

            # Condition if None everywhere
            if not title_recipe and not budget and not level and not rating and not time and not categories and not number_of_person:
                title_recipe = 0
                budget = 0
                level = 0
                rating = 0
                time_total = 0
                time_prepa = 0
                time_cooking = 0
                categorie_str = 0
                number_of_person = 0

                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                                    values ('%s','%d','%s','%s','%s','%s','%s','%s','%s');""" % (
                title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget,
                categorie_str)

                sql_update_recipe = """update recipe set (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                                    values ('%s','%d','%s','%s','%s','%s','%s','%s','%s');""" % (
                title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget,
                categorie_str)

            ############### Table Recipe

            # Condition for Categories
            if categories is None:
                categories = 'Vide'
                print( "0 categorie" )
            else:
                pass

            # Condition if Budget is None
            if budget is None:
                budget = 0
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget,categorie_str)

                #sql_update_recipe = """update recipe set (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                 #                                   values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget,categorie_str)

            # Condition if rating is None
            if rating is None:
                rating = 0
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget,categorie_str)


            # Condition if Time is None
            elif time is None:
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                title_recipe, level, number_of_person, rating, 0, 0, 0, budget, categorie_str)

                #sql_update_recipe = """update recipe set (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                 #                                   values ('%s','%d','%s','%s','%s','%s','%s','%s','%s');""" % (
                  #  title_recipe, level, number_of_person, rating, 0, 0, 0, budget,categorie_str)

            elif time_total is None:
                time_prepa = values.get( 'time' ).get( 'preparation' )
                time_cooking = values.get( 'time' ).get( 'cooking' )
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                    title_recipe, level, number_of_person, rating, 0, time_prepa, time_cooking, budget, categorie_str)

            elif time_prepa is None:
                time_total = values.get( 'time' ).get( 'total' )
                time_cooking = values.get( 'time' ).get( 'cooking' )
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                    title_recipe, level, number_of_person, rating, time_total, 0, time_cooking, budget, categorie_str)

            elif time_cooking is None:
                time_total = values.get( 'time' ).get( 'total' )
                time_prepa = values.get( 'time' ).get( 'preparation' )
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                    title_recipe, level, number_of_person, rating, time_total, time_prepa, 0, budget, categorie_str)

            # Time == full
            else:
                time_total = values.get( 'time' ).get( 'total' )
                time_prepa = values.get( 'time' ).get( 'preparation' )
                time_cooking = values.get( 'time' ).get( 'cooking' )

                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\

                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget,categorie_str)

                sql_update_recipe = """update recipe set (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories)\
                                                    values ('%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                    title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget,categorie_str)

            print( "RECIPE ::", sql_recipe )

            # Injection des recipes en base
            try:
                # Execute the SQL command
                self.cursor.execute( sql_recipe )
                # Commit your changes in the database
                self.db.commit()
            except:
                # Rollback in case there is any error
                self.db.rollback()


########## Création table  liaison

        for values in recipes.recipes:

            steps = values.get( 'etapes' )
            ingredients = values.get( 'ingredients' )
            categories = values.get( 'categories' )
            title_recipe = values.get( 'title' )
            utensils = values.get( 'utensils' )

            ## Construction table l_recipe_ingredient
            if ingredients is None:
                pass
                print( "0 ingrédients" )
            else:
                for ing in ingredients:
                    ing_id_unique = 0
                    ing_quantity = ing.get( 'quantity' )
                    ing_name = re.escape( str( ing.get( 'name' ) ) )
                    sql_get_id_ingredient = """select id_ingredient from ingredient where name_ingredient= '%s';""" % (
                        ing_name)
                    print( "SQL GET ID INGRE :::::::::", sql_get_id_ingredient )
                    sql_get_id_recipe = """select id_recipe from recipe where title_recipe= '%s';""" % (title_recipe)
                    print( "GET ID RECIPE :", sql_get_id_recipe )
                    id_fetch_rec = 0
                    id_fetch = 0

                    try:
                        self.cursor.execute(
                            """select id_recipe from recipe where title_recipe= '%s';""" % (title_recipe) )
                        id_rec = self.cursor.fetchall()
                        for id_rec_att in id_rec:
                            id_fetch_rec = id_rec_att[0]
                            print( "CLE PRIMAIRE RECIPE :", id_fetch_rec )
                            print("TITRE RECIPE :::::", title_recipe)
                    except:
                        print( "Error get ID recipe" )
                        self.db.rollback()

                    try:
                        self.cursor.execute(
                            """select id_ingredient from ingredient where name_ingredient= '%s';""" % (ing_name) )
                        id_ingre = self.cursor.fetchall()
                        for id_att in id_ingre:
                            id_fetch = id_att[0]
                            print( "CLE PRIMAIRE ING = ", id_fetch )
                    except:
                        print( "Error get ID Ingredient" )
                        self.db.rollback()

                    if ing_quantity is None:
                        sql_l_recipe_ingredient = """ insert into l_recipe_ingredient (id_recipe,id_ingredient,title_recipe,name_ingredient,quantity_ingredient) values('%s','%s','%s','%s','%s');""" % (
                            id_fetch_rec, id_fetch, title_recipe, ing_name, None)
                        print( "LIAISON INGREDIENT :", sql_l_recipe_ingredient )

                    else:
                        sql_l_recipe_ingredient = """ insert into l_recipe_ingredient (id_recipe,id_ingredient,title_recipe,name_ingredient,quantity_ingredient) values('%s','%s','%s','%s','%s');""" % (
                            id_fetch_rec, id_fetch, title_recipe, ing_name, ing_quantity)
                        print( "LIAISON INGREDIENT :", sql_l_recipe_ingredient )

                    try:
                            # Execute the SQL command
                        self.cursor.execute( sql_l_recipe_ingredient )
                            # Commit your changes in the database
                        self.db.commit()
                    except:
                            # Rollback in case there is any error
                        self.db.rollback()

            # Table Liaison unique step
            if steps is None:
                pass
                print( "0 étape" )
            else:
                for step in steps:
                    sql_step = ""
                    step_num = step['Etape']
                    step_desc = re.escape( str( step['Description'] ) )
                    sql_step = """insert into step (id_recipe,title_recipe,step_number,description_step) values ('%s','%s','%s','%s');""" % (
                            id_fetch_rec, title_recipe, step_num, step_desc)
                    print("LIAISON STEP ::",sql_step)
                        # Injection des steps
                    try:
                            # Execute the SQL command
                        self.cursor.execute( sql_step )
                            # Commit your changes in the database
                        self.db.commit()
                    except:
                            # Rollback in case there is any error
                        self.db.rollback()

                # Table Liaison Utensils
            if utensils is None:
                pass
                print( "0 utensils" )
            else:
                print( "Nombre d'utensils :", len( utensils ) )

                for utensil in utensils:
                    sql_get_id_utensil = """ select id_utensil from utensil where name_utensil='%s';""" % (utensil)
                    print( "SQL GET UTENSIL :", sql_get_id_utensil )

                    try:
                        self.cursor.execute(
                            """select id_utensil from utensil where name_utensil='%s';""" % (utensil) )
                        id_uten = self.cursor.fetchall()
                        for id_utensil in id_uten:
                            id_uten_fetch = id_utensil[0]
                            print( "CLE PRIMAIRE INGREDIENT = ", id_uten_fetch )
                    except:
                        print( "Error: unable to get ingredient id" )
                        self.db.rollback()

                    sql_l_recipe_utensils = """ insert into l_recipe_utensil (id_recipe,id_utensil,name_utensil,title_recipe) values ('%s','%s','%s','%s');""" % (
                            id_fetch_rec, id_uten_fetch, utensil, title_recipe)
                    print( "LIAISON UTENSIL ::", sql_l_recipe_utensils )
                        # Injection des utensils
                    try:
                            # Execute the SQL command
                        self.cursor.execute( sql_l_recipe_utensils )
                            # Commit your changes in the database
                        self.db.commit()
                    except:
                            # Rollback in case there is any error
                        self.db.rollback()

        self.disconnect()

        print( "Nombre d'utensils", len( list_utensil_total_pd ) )
        print( "Nombre d'ingrédients : ", len( liste_ingre_totale_pd ) )
        print( "La Base de données a bien été importée" )

    def truncate_db(self):

        self.connect()
        sql_t_l_recipe_ingredient = "drop table l_recipe_ingredient;"
        sql_t_l_recipe_utensil = "drop table l_recipe_utensil;"
        sql_step =  "drop table step;"
        sql_recipe = "truncate table recipe;"
        sql_utensil = "truncate table utensil;"
        sql_ingredient = "truncate table ingredient;"
        sql_categorie = "truncate table categorie;"
        sql_sport_activity = "truncate table sport_activity;"
        sql_user_profil = "drop table user_profile;"

        try:
            # Execute the SQL command
            self.cursor.execute( sql_t_l_recipe_utensil )
            self.cursor.execute( sql_t_l_recipe_ingredient )
            self.cursor.execute( sql_recipe )
            self.cursor.execute( sql_utensil )
            self.cursor.execute( sql_step )
            self.cursor.execute( sql_ingredient )
            self.cursor.execute( sql_categorie )
            self.cursor.execute( sql_sport_activity )
            self.cursor.execute( sql_user_profil )
            # Commit your changes in the database
            self.db.commit()

        except:
            # Rollback in case there is any error
            self.db.rollback()
        self.disconnect()
        print( "Database bcd is empty" )