# If MySQLdb not installed


# pip install mysqlclient

import MySQLdb

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
                        SET FOREIGN_KEY_CHECKS=0;
                        DROP TABLE IF EXISTS L_RECIPE_INGREDIENT;
                        DROP TABLE IF EXISTS L_RECIPE_UTENSIL;
                        DROP TABLE IF EXISTS L_RECIPE_LABEL;
                        DROP TABLE IF EXISTS STEP;
                        DROP TABLE IF EXISTS RECIPE;
                        DROP TABLE IF EXISTS INGREDIENT;
                        DROP TABLE IF EXISTS USER_PROFILE;
                        DROP TABLE IF EXISTS UTENSIL; 
                        DROP TABLE IF EXISTS CATEGORIE;
                        DROP TABLE IF EXISTS LABEL;
                        SET FOREIGN_KEY_CHECKS=1;
                        
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
                        CATEGORIES VARCHAR(255),
                        LABEL VARCHAR(255));
                        
                        CREATE TABLE CATEGORIE (
                        ID_CATEGORIE INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                        CATEGORIE_NAME VARCHAR(255));
                        
                        CREATE TABLE LABEL (
                        ID_LABEL INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                        LABELNAME VARCHAR(255));

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
                            
                        CREATE TABLE L_RECIPE_CATEGORIE (
                        ID_RECIPE INT NOT NULL,
                        ID_CATEGORIE INT NOT NULL,
                        TITLE_RECIPE VARCHAR(255),
                        NAME_CATEGORIE VARCHAR(255),
                        CONSTRAINT cst_r_c
                            FOREIGN KEY (ID_RECIPE) REFERENCES RECIPE(ID_RECIPE),
                            FOREIGN KEY (ID_CATEGORIE) REFERENCES CATEGORIE(ID_CATEGORIE));
                            
                        CREATE TABLE L_RECIPE_LABEL (
                        ID_RECIPE INT NOT NULL,
                        ID_LABEL INT NOT NULL,
                        TITLE_RECIPE VARCHAR(255),
                        CONSTRAINT cts_r_l
                            FOREIGN KEY (ID_RECIPE) REFERENCES RECIPE(ID_RECIPE),
                            FOREIGN KEY (ID_LABEL) REFERENCES LABEL(ID_LABEL));
                        

                        CREATE TABLE USER_PROFILE (
                        ID_USER INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                        USERNAME VARCHAR(50),
                        BIRTHDAY_USER DATETIME,
                        MAIL_USER VARCHAR(100),
                        CATEGORIES VARCHAR(255),
                        REGISTRATION_DATE DATETIME);

                        CREATE TABLE STEP (
                        ID_STEP INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                        ID_RECIPE INT,
                        TITLE_RECIPE VARCHAR(255),
                        STEP_NUMBER FLOAT,
                        DESCRIPTION_STEP VARCHAR(255),
                        CONSTRAINT cst_s
                            FOREIGN KEY (ID_RECIPE) REFERENCES RECIPE(ID_RECIPE));"""



        sql_drop = "drop table l_recipe_ingredient;" \
                   "drop table l_recipe_utensil;" \
                   "drop table l_recipe_categorie;" \
                   "drop table l_recipe_label;" \
                   "drop table step;" \
                   "drop table recipe;" \
                   "drop table utensil;" \
                   "drop table ingredient;" \
                   "drop table categorie;" \
                   "drop table user_profile;" \
                   "drop table label;"

        _SQL = """SHOW TABLES"""
        self.cursor.execute( _SQL )
        results = self.cursor.fetchall()

        table_l_r_ingredient = 'l_recipe_ingredient'
        table_l_r_utensil = 'l_recipe_utensil'
        table_l_s = 'step'
        table_l_r_c = 'l_recipe_categorie'
        table_l_r_l = 'l_recipe_label'

        print( 'All existing tables:', results )  # Returned as a list of tuples
        results_list = [item[0] for item in results]  # Conversion to list of str

        if table_l_r_ingredient and table_l_r_utensil and table_l_s and table_l_r_c and table_l_r_l in results_list:
            print( table_l_r_ingredient,table_l_r_utensil,table_l_s,table_l_r_c ,'was found!' )

            try:
                self.cursor.execute(sql_drop)
                self.db.commit()
            except:
                self.db.rollback()

        else:
            print( table_l_r_ingredient,table_l_r_utensil,table_l_s,table_l_r_c, 'was NOT found!' )
            try:
                self.cursor.execute( sql_create_db )
            except:
                self.db.rollback()

        self.disconnect()

        print( "La Base de données a bien été créée" )


### BUILD TABLE INGREDIENT

    def build_ingredient(self,recipes):

        global ing_id_unique
        liste_ingre_totale_pd  = []
        liste_ingre_totale = []
        compteur=0

        self.connect()

        for values in recipes.recipes:
            ingredients = values.get( 'ingredients' )
            title = values.get( 'title' )
            title_rec_tmp = ''.join( str( title ) )
            title_recipe = re.escape( str( title_rec_tmp ) )
            print( "--- NUMEROUS RECIPE :", compteur, ":", title_recipe, "---" )
            print("Ingredient Panda :", len( liste_ingre_totale_pd ) )
            print( "LEN Lst tmp :", len( liste_ingre_totale ) )
            compteur += 1

            if ingredients is None:
                pass
                print( "0 ingrédients" )
            else:
                for ing in ingredients:
                    liste_ingre = []
                    ing_id_unique = 0
                    ing_name = re.escape( str( ing.get( 'name' ) ) )
                    liste_ingre.append( ing_name )
                    liste_ingre_totale.extend( liste_ingre )
                    liste_ingre_totale_pd = pd.Series( liste_ingre_totale ).drop_duplicates().tolist()
                    if len(liste_ingre_totale) == 2500:
                        liste_ingre_totale = liste_ingre_totale_pd

        for ingre in liste_ingre_totale_pd:
            ing_id_unique += 1
            sql_ingredient = """ insert into ingredient (name_ingredient,ing_id) values('%s','%s');""" % (ingre, ing_id_unique)
            print( "INGREDIENT :", sql_ingredient )
            print(len(liste_ingre_totale_pd))
            ing_id_unique+1
            try:
                # Execute the SQL command
                self.cursor.execute( sql_ingredient )
                # Commit your changes in the database
                self.db.commit()
            except:
                # Rollback in case there is any error
                self.db.rollback()

#BUILD TABLE CATEGORIE

    def build_categorie(self,recipes):
        global categorie_id_unique
        compteur = 0
        list_categorie_total_pd = []
        list_categorie_total = []

        self.connect()

        for values in recipes.recipes:
            categories = values.get( 'categories' )
            title = values.get( 'title' )
            title_rec_tmp = ''.join( str( title ) )
            title_recipe = re.escape( str( title_rec_tmp ) )
            print( "--- NUMEROUS RECIPE :", compteur, ":", title_recipe, "---" )
            compteur += 1

            if categories is None:
                pass
                print( "0 catégories" )
            else:
                for categorie in categories:
                    categorie.replace("[", "")
                    categorie.replace("]", "")
                    categorie = ''.join( str( categorie ) )
                    re.escape(str(categorie))
                    list_categorie = []
                    list_categorie.append( categorie )
                    list_categorie_total.extend( list_categorie )
                    list_categorie_total_pd = pd.Series( list_categorie_total ).drop_duplicates().tolist()
                    print("LEN list categorie", len(list_categorie_total_pd))
                    print("LEN Liste tmp", len(list_categorie_total))
                    if len(list_categorie_total)>= 5000:
                        list_categorie_total = list_categorie_total_pd

        for categ in list_categorie_total_pd:
            categ.replace( "[", "" )
            categ.replace( "]", "" )
            categ.split( "," )
            categ_tmp = ''.join( str( categ ) )
            category = re.escape( str( categ_tmp ) )
            sql_categorie = """ insert into categorie(categorie_name) values ('%s'); """ % (category)
            print (" CATEGORIE :", sql_categorie)
            print(len(list_categorie_total_pd))
            try:
                # Execute the SQL command
                self.cursor.execute( sql_categorie )
                # Commit your changes in the database
                self.db.commit()
            except:
                # Rollback in case there is any error
                self.db.rollback()

        self.disconnect()


    def build_utensil(self, recipes):

        self.connect()

        global sql_recipe, sql_step, sql_ingredient, sql_utensils, liste_ingre_totale_pd, categories, categorie_str, liste_ingre \
            , utensils, sql_l_recipe_ingredient, dict_ingre, ing_id_unique, ing_quantity, dict_utensil, list_utensil_total_pd, utensil_id_unique, utensil, title_recipe, list_utensil_by_recipe, ingre, id_fetch, id_uten_fetch, \
            id_fetch_rec, list_categorie_total_pd, categorie_id_unique
        compteur = 0

        list_utensil_total = []
        list_utensil_total_pd = []
        dict_ingre = {}

        self.connect()

        ################# Création table ingredients, utensils et catégorie

        for values in recipes.recipes:

            title = values.get( 'title' )
            title_rec_tmp = ''.join(str(title))
            title_recipe = re.escape(str(title_rec_tmp))
            utensils = values.get( 'utensils' )
            compteur += 1
            print( "--- NUMEROUS RECIPE :", compteur, ":", title_recipe, "---" )
            print( "LEN LISTE UTENSIL :", len( list_utensil_total_pd ) )

            # Création d'un dictionnaire utensils sans doublons
            if utensils is None:
                pass
                print( "0 utensils" )
            else:
                for utensil in utensils:
                    utensil_id_unique = 1
                    list_utensil_total.extend( utensils )
                    list_utensil_total_pd = pd.Series( list_utensil_total ).drop_duplicates().tolist()
                    if len(list_utensil_total)>= 5000:
                        list_utensil_total = list_utensil_total_pd
        print( "Nombre d'utensils", len( list_utensil_total_pd ))
        print ("LEN Liste tmp :", list_utensil_total)

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

        self.disconnect()

#############
#
# Création table recipe
#
#############

    def build_recipe(self,recipes):
        compteur = 0
        global sql_recipe

        self.connect()

        for values in recipes.recipes:

            number_of_person = values.get( 'number_of_person' )
            time = values.get( 'time' )
            rating = values.get( 'rating' )
            level = values.get( 'level' )
            budget = values.get( 'budget' )
            title_recipe = values.get( 'title' )
            categories = values.get('categories')
            print("CATEGORIES :",categories)
            categorie_tmp = str(categories).strip('[,],\'')
            print("CATEGORIES_TMP :",categorie_tmp)
            categorie_str = re.escape((str(categorie_tmp)))
            print("CATEGORIES_STR :",categorie_str)
            utensils = values.get('utensils')
            label = values.get('label_cluster')
            time_total = 0
            time_prepa = 0
            time_cooking = 0

            # Affichage numerous recipe et compteur
            compteur = compteur + 1
            print( "--- NUMEROUS RECIPE :", compteur, ":", title_recipe, "---" )

            # Condition if None everywhere
            if not title_recipe and not budget and not level and not rating and not time and not categories and not number_of_person and not label:
                title_recipe = 0
                budget = 0
                level = 0
                rating = 0
                time_total = 0
                time_prepa = 0
                time_cooking = 0
                categorie_str = 0
                number_of_person = 0
                label = 0

            # Condition for Categories
            if categories is None:
                print( "0 categorie" )

            if label is None:
                print("0 label")

            if number_of_person is None :
                print("0 personnes")

            if utensils is None:
                pass
                print( "0 utensils" )

            if rating and budget and label is None:
                rating = 0
                budget = 0
                label = 0
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories,label)\
                           values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget,categorie_str,label)

            if time is None:
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories,label)\
                           values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                    title_recipe, level, number_of_person, rating, 0, 0, 0, budget, categorie_str,label)

            elif time_total is None:
                time_prepa = values.get( 'time' ).get( 'preparation' )
                time_cooking = values.get( 'time' ).get( 'cooking' )
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories,label)\
                                           values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                    title_recipe, level, number_of_person, rating, 0, time_prepa, time_cooking, budget, categorie_str,label)

            elif time_prepa is None:
                time_total = values.get( 'time' ).get( 'total' )
                time_cooking = values.get( 'time' ).get( 'cooking' )
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories,label)\
                                           values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                    title_recipe, level, number_of_person, rating, time_total, 0, time_cooking, budget, categorie_str,label)

            elif time_cooking is None:
                time_total = values.get( 'time' ).get( 'total' )
                time_prepa = values.get( 'time' ).get( 'preparation' )
                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories,label)\
                                           values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                    title_recipe, level, number_of_person, rating, time_total, time_prepa, 0, budget, categorie_str,label)

            else:
                time_total = values.get( 'time' ).get( 'total' )
                time_prepa = values.get( 'time' ).get( 'preparation' )
                time_cooking = values.get( 'time' ).get( 'cooking' )

                sql_recipe = """insert into recipe (title_recipe,level_recipe,number_of_person,rating_recipe,time_total,time_prepa,time_cooking,budget,categories,label)\

                           values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
                    title_recipe, level, number_of_person, rating, time_total, time_prepa, time_cooking, budget,categorie_str,label)

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

        self.disconnect()

        print( "La Base de données a bien été importée" )

### Fonction TRUNCATE
    def truncate_db(self):

        self.connect()

        sql_truncate = "SET FOREIGN_KEY_CHECKS=0;" \
                       "truncate table l_recipe_ingredient;" \
                       "truncate table l_recipe_utensil;" \
                       "truncate table l_recipe_label;" \
                       "truncate table l_recipe_categorie;" \
                       "truncate table step;" \
                       "truncate table recipe;" \
                       "truncate table utensil;" \
                       "truncate table ingredient;" \
                       "truncate table categorie;" \
                       "truncate table user_profile;" \
                       "SET FOREIGN_KEY_CHECKS=1;"

        try:
            self.cursor.execute( sql_truncate )
            self.db.commit()
        except:
            self.db.rollback()
        self.disconnect()

        print( "Database bcd is empty" )


########## Création table  liaison : ingredient,categorie,utensils,step
    def build_tl(self,recipes):
# Table liaison recipe ingredient
        self.connect()
        global id_uten_fetch, id_categ_fetch, id_fetch_rec
        for values in recipes.recipes:
            steps = values.get( 'etapes' )
            ingredients = values.get( 'ingredients' )
            categories = values.get( 'categories' )
            title_recipe = values.get( 'title' )
            utensils = values.get( 'utensils' )

            if ingredients is None:
                pass
                print( "0 ingrédients" )
            else:
                for ing in ingredients:
                    ing_quantity = ing.get( 'quantity' )
                    ing_prefix = ing.get('prefix')
                    ing_name = re.escape( str( ing.get( 'name' ) ) )
                    sql_get_id_ingredient = """select id_ingredient from ingredient where name_ingredient= '%s';""" % (
                        ing_name)
                    print( "SQL GET ID INGRE :::::::::", sql_get_id_ingredient )
                    sql_get_id_recipe = """select id_recipe from recipe where title_recipe= '%s';""" % (title_recipe)
                    print( "GET ID RECIPE :", sql_get_id_recipe )
                    id_fetch_rec = 0
                    id_fetch = 0
                    print("PREFIXXXXXX :",ing_prefix)
                    try:
                        self.cursor.execute(
                            """select id_recipe from recipe where title_recipe= '%s';""" % (title_recipe) )
                        id_rec = self.cursor.fetchall()
                        for id_rec_att in id_rec:
                            id_fetch_rec = id_rec_att[0]
                            print( "CLE PRIMAIRE RECIPE :", id_fetch_rec )
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

                    if ing_prefix is None:
                        ing_prefix=0

                    if ing_quantity is None:
                        sql_l_recipe_ingredient = """ insert into l_recipe_ingredient (id_recipe,id_ingredient,title_recipe,name_ingredient,quantity_ingredient,prefix) values('%s','%s','%s','%s','%s','%s');""" % (
                            id_fetch_rec, id_fetch, title_recipe, ing_name, None,ing_prefix)
                        print( "LIAISON INGREDIENT :", sql_l_recipe_ingredient )

                    else:
                        sql_l_recipe_ingredient = """ insert into l_recipe_ingredient (id_recipe,id_ingredient,title_recipe,name_ingredient,quantity_ingredient,prefix) values('%s','%s','%s','%s','%s','%s');""" % (
                            id_fetch_rec, id_fetch, title_recipe, ing_name, ing_quantity,ing_prefix)
                        print( "LIAISON INGREDIENT :", sql_l_recipe_ingredient )

                    try:
                        # Execute the SQL command
                        self.cursor.execute( sql_l_recipe_ingredient )
                        # Commit your changes in the database
                        self.db.commit()
                    except:
                        # Rollback in case there is any error
                        self.db.rollback()

            if steps is None:
                pass
                print( "0 étape" )
            else:
                for step in steps:
                    step_num = step['Etape']
                    step_desc = re.escape( str( step['Description'] ) )
                    sql_step = """insert into step (id_recipe,title_recipe,step_number,description_step) values ('%s','%s','%s','%s');""" % (
                        id_fetch_rec, title_recipe, step_num, step_desc)
                    print( "LIAISON STEP ::", sql_step )
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
                        self.cursor.execute( sql_get_id_utensil )
                        id_uten = self.cursor.fetchall()
                        for id_utensil in id_uten:
                            id_uten_fetch = id_utensil[0]
                            print( "CLE PRIMAIRE UTENSIL = ", id_uten_fetch )
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

# Table Liaison Categorie
            if categories is None:
                pass
                print( "0 catégorie" )
            else:
                print( "Nombre de catégorie :", len( categories ) )

                for categorie in categories:
                    sql_get_id_categorie = """ select id_categorie from categorie where categorie_name ='%s';""" % (
                        categorie)
                    print( "SQL GET CATEGORIE :", sql_get_id_categorie )

                    try:
                        self.cursor.execute( sql_get_id_categorie )
                        id_categ = self.cursor.fetchall()
                        for id_categorie in id_categ:
                            id_categ_fetch = id_categorie[0]
                            print( "CLE PRIMAIRE CATEGORIE = ", id_categ_fetch )
                    except:
                        print( "Error: unable to get categorie id" )
                        self.db.rollback()

                    sql_l_recipe_categorie = """ insert into l_recipe_categorie (id_recipe,id_categorie,title_recipe, name_categorie) values ('%s','%s','%s','%s');""" % (
                        id_fetch_rec, id_categ_fetch, title_recipe, categorie)
                    print( "LIAISON CATEGORIE ::", sql_l_recipe_categorie )
                    # Injection des utensils
                    try:
                        # Execute the SQL command
                        self.cursor.execute( sql_l_recipe_categorie )
                        # Commit your changes in the database
                        self.db.commit()
                    except:
                        # Rollback in case there is any error
                        self.db.rollback()

        self.disconnect()
        print( "La Base de données a bien été importée" )

