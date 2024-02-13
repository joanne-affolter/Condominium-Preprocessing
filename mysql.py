import pymysql
import pandas as pd

class MySQL:
    def __init__(self, host, user, password, db):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db)
        self.cursor = self.connection.cursor()

    def close(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()

    def create_table(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except pymysql.Error as e:
            print(f"Error creating table: {e}")

    def insert(self, query, values):
        try:
            self.cursor.execute(query, values)
            self.connection.commit()
        except pymysql.Error as e:
            print(f"Error inserting data: {e}")
    
    def select(self, query, values):
        try:
            self.cursor.execute(query, values)
            res = self.cursor.fetchall()
            return res
        
        except pymysql.Error as e:
            print(f"Error selecting data: {e}")
    
    def update(self, query, values):
        try:
            self.cursor.execute(query, values)
            self.connection.commit()
        except pymysql.Error as e:
            print(f"Error updating data: {e}")

    def create_Syndics(self):

        create_table_query = '''
            CREATE TABLE IF NOT EXISTS Syndics (
                SIRET VARCHAR(14) PRIMARY KEY,
                Nom VARCHAR(256),
                Autre_Denomination VARCHAR(256),
                Catégorie VARCHAR(256),
                Description VARCHAR(1024),
                Adresse VARCHAR(1024),
                Code_Postal VARCHAR(5),
                Ville VARCHAR(256),
                Code_Commune VARCHAR(5),
                Code_Pays VARCHAR(5),
                Departement VARCHAR(5),
                Telephones VARCHAR(256),
                URL_PagesJaunes VARCHAR(256),
                URL_Solocal VARCHAR(256),
                URL_Linkedin_Entreprise VARCHAR(256),
                ID_Linkedin_Entreprise VARCHAR(256),
                Forme_Juridique_INSEE INT, 
                Code_APE VARCHAR(10),
                Date_Creation DATE,
                Effectif VARCHAR(256),
                Typologie VARCHAR(256)
            )
            '''
        self.create_table(create_table_query)

    def add_data_Syndics(self, syndics_path):
        
        syndics_df = pd.read_csv(syndics_path, dtype={'SIRET': str})
        syndics_df.fillna('', inplace=True)

        for index, row in syndics_df.iterrows():
            #Make a get request to check if the SIRET already exists in the database
            query = "SELECT * FROM Syndics WHERE SIRET = %s"
            values = (row['SIRET'],)
            res = mysql.select(query, values)
            
            #Check if empty result
            if res == () :  #Empty --> add row
                query = '''
                    INSERT INTO `Syndics` 
                    (`SIRET`, `Nom`, `Autre_Denomination`, `Catégorie`, `Description`, `Adresse`, `Code_Postal`, `Ville`, `Code_Commune`, `Code_Pays`, `Departement`, `Telephones`, `URL_PagesJaunes`, `URL_Solocal`, `URL_Linkedin_Entreprise`, `ID_Linkedin_Entreprise`, `Forme_Juridique_INSEE`, `Code_APE`, `Date_Creation`, `Effectif`, `Typologie`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                values = (row['SIRET'], row['Nom'], row['Autre_Denomination'], row['Catégorie'], row['Description'], row['Adresse'], row['Code_Postal'], row['Ville'], row['Code_Commune'], row['Code_Pays'], row['Departement'], row['Telephones'], row['URL_PagesJaunes'], row['URL_Solocal'], row['URL_Linkedin_Entreprise'], row['ID_Linkedin_Entreprise'], row['Forme_Juridique_INSEE'], row['Code_APE'], row['Date_Creation'], row['Effectif'], row['Typologie'])
                mysql.insert(query, values)
            else :  #Not empty --> update row
                query = '''
                    UPDATE `Syndics` 
                    SET `Nom` = %s, `Autre_Denomination` = %s, `Catégorie` = %s, `Description` = %s, `Adresse` = %s, `Code_Postal` = %s, `Ville` = %s, `Code_Commune` = %s, `Code_Pays` = %s, `Departement` = %s, `Telephones` = %s, `URL_PagesJaunes` = %s, `URL_Solocal` = %s, `URL_Linkedin_Entreprise` = %s, `ID_Linkedin_Entreprise` = %s, `Forme_Juridique_INSEE` = %s, `Code_APE` = %s, `Date_Creation` = %s, `Effectif` = %s, `Typologie` = %s
                    WHERE `SIRET` = %s
                '''
                values = (row['Nom'], row['Autre_Denomination'], row['Catégorie'], row['Description'], row['Adresse'], row['Code_Postal'], row['Ville'], row['Code_Commune'], row['Code_Pays'], row['Departement'], row['Telephones'], row['URL_PagesJaunes'], row['URL_Solocal'], row['URL_Linkedin_Entreprise'], row['ID_Linkedin_Entreprise'], row['Forme_Juridique_INSEE'], row['Code_APE'], row['Date_Creation'], row['Effectif'], row['Typologie'], row['SIRET'])
                mysql.update(query, values)
    
    def create_Copros(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS Copros (
                Immatriculation_Copro VARCHAR(32) PRIMARY KEY,
                Nom VARCHAR(256),
                Rue VARCHAR(256),
                Code_Postal INT,
                Ville VARCHAR(256),
                Code_Commune_INSEE VARCHAR(32),
                Departement INT,
                Longitude VARCHAR(8),
                Latitude VARCHAR(8),
                Nombre_Total_Lots INT,
                Nombre_Total_Lots_HBC INT,
                Nombre_Total_Lots_Habitation INT,
                Nombre_Total_Lots_Stationnement INT,
                Residence_Service INT,
                Syndicat_Cooperatif INT,
                Syndicat_Principal INT,
                Immatriculation_Syndicat_Principal VARCHAR(64), 
                Nombre_ASL INT,
                Nombre_AFUL INT,
                Nombre_Union_Syndicat INT,
                Debut_Construction DATE,
                Fin_Construction DATE,
                Reference_Cadastrale VARCHAR(64),
                Copro_Dans_ACV INT,
                Copro_Dans_PVD INT,
                Copro_Aidee INT,
                Code_EPCI INT,
                SIRET_Syndic VARCHAR(14),
                Type_Syndic VARCHAR(32),
                Nom_Syndic VARCHAR(256),
                Match_SIRET INT,
                FOREIGN KEY (SIRET_Syndic) REFERENCES Syndics(SIRET)
            )
            '''
        self.create_table(create_table_query)
    
    def add_data_Copros(self, copros_path):
        copros_df = pd.read_csv(copros_path, dtype={'Immatriculation_Copro': str, 'Immatriculation_Syndicat_Principal': str, 'SIRET_Syndic': str})
        copros_df.fillna('', inplace=True)

        for index, row in copros_df.iterrows():
            #Make a get request to check if the Immatriculation_Copro already exists in the database
            query = "SELECT * FROM Copros WHERE Immatriculation_Copro = %s"
            values = (row['Immatriculation_Copro'],)
            res = mysql.select(query, values)
            
            #Check if empty result
            if res == () :  #Empty --> add row
                query = '''
                    INSERT INTO `Copros` 
                    (`Immatriculation_Copro`, `Nom`, `Rue`, `Code_Postal`, `Ville`, `Code_Commune_INSEE`, `Departement`, `Longitude`, `Latitude`, `Nombre_Total_Lots`, `Nombre_Total_Lots_HBC`, `Nombre_Total_Lots_Habitation`, `Nombre_Total_Lots_Stationnement`, `Residence_Service`, `Syndicat_Cooperatif`, `Syndicat_Principal`, `Immatriculation_Syndicat_Principal`, `Nombre_ASL`, `Nombre_AFUL`, `Nombre_Union_Syndicat`, `Debut_Construction`, `Fin_Construction`, `Reference_Cadastrale`, `Copro_Dans_ACV`, `Copro_Dans_PVD`, `Copro_Aidee`, `Code_EPCI`, `SIRET_Syndic`, `Type_Syndic`, `Nom_Syndic`, `Match_SIRET`)
                    VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                '''
                values = (row['Immatriculation_Copro'], row['Nom'], row['Rue'], row['Code_Postal'], row['Ville'], row['Code_Commune_INSEE'], row['Departement'], row['Longitude'], row['Latitude'], row['Nombre_Total_Lots'], row['Nombre_Total_Lots_HBC'], row['Nombre_Total_Lots_Habitation'], row['Nombre_Total_Lots_Stationnement'], row['Residence_Service'], row['Syndicat_Cooperatif'], row['Syndicat_Principal'], row['Immatriculation_Syndicat_Principal'], row['Nombre_ASL'], row['Nombre_AFUL'], row['Nombre_Union_Syndicat'], row['Debut_Construction'], row['Fin_Construction'], row['Reference_Cadastrale'], row['Copro_Dans_ACV'], row['Copro_Dans_PVD'], row['Copro_Aidee'], row['Code_EPCI'], row['SIRET_Syndic'], row['Type_Syndic'], row['Nom_Syndic'], row['Match_SIRET'])
                mysql.insert(query, values)
            else :  #Not empty --> update row
                query = '''
                    UPDATE `Copros` 
                    SET `Nom` = %s, `Rue` = %s, `Code_Postal` = %s, `Ville` = %s, `Code_Commune_INSEE` = %s, `Departement` = %s, `Longitude` = %s, `Latitude` = %s, `Nombre_Total_Lots` = %s, `Nombre_Total_Lots_HBC` = %s, `Nombre_Total_Lots_Habitation` = %s, `Nombre_Total_Lots_Stationnement` = %s, `Residence_Service` = %s, `Syndicat_Cooperatif` = %s, `Syndicat_Principal` = %s, `Immatriculation_Syndicat_Principal` = %s, `Nombre_ASL` = %s, `Nombre_AFUL` = %s, `Nombre_Union_Syndicat` = %s, `Debut_Construction` = %s, `Fin_Construction` = %s, `Reference_Cadastrale` = %s, `Copro_Dans_ACV` = %s, `Copro_Dans_PVD` = %s, `Copro_Aidee` = %s, `Code_EPCI` = %s, `SIRET_Syndic` = %s, `Type_Syndic` = %s, `Nom_Syndic` = %s, `Match_SIRET` = %s
                    WHERE `Immatriculation_Copro` = %s
                '''
                values = (row['Nom'], row['Rue'], row['Code_Postal'], row['Ville'], row['Code_Commune_INSEE'], row['Departement'], row['Longitude'], row['Latitude'], row['Nombre_Total_Lots'], row['Nombre_Total_Lots_HBC'], row['Nombre_Total_Lots_Habitation'], row['Nombre_Total_Lots_Stationnement'], row['Residence_Service'], row['Syndicat_Cooperatif'], row['Syndicat_Principal'], row['Immatriculation_Syndicat_Principal'], row['Nombre_ASL'], row['Nombre_AFUL'], row['Nombre_Union_Syndicat'], row['Debut_Construction'], row['Fin_Construction'], row['Reference_Cadastrale'], row['Copro_Dans_ACV'], row['Copro_Dans_PVD'], row['Copro_Aidee'], row['Code_EPCI'], row['SIRET_Syndic'], row['Type_Syndic'], row['Nom_Syndic'], row['Match_SIRET'], row['Immatriculation_Copro'])
                mysql.update(query, values)

    def create_Tags(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS Tags (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Tag VARCHAR(256)
            )
            '''
        self.create_table(create_table_query)
    
    def add_data_Tags(self, tags_path):
        tags_df = pd.read_csv(tags_path)
        tags_df.fillna('', inplace=True)

        for index, row in tags_df.iterrows():
            #Make a get request to check if the Tag already exists in the database
            query = "SELECT * FROM Tags WHERE Tag = %s"
            values = (row['Service'],)
            res = mysql.select(query, values)
            
            #Check if empty result
            if res == () :  #Empty --> add row
                query = '''
                    INSERT INTO `Tags` 
                    (`Tag`)
                    VALUES (%s)
                '''
                values = (row['Service'],)
                mysql.insert(query, values)
            else :  #Not empty --> update row
                query = '''
                    UPDATE `Tags` 
                    SET `Tag` = %s
                    WHERE `Tag` = %s
                '''
                values = (row['Service'], row['Service'])
                mysql.update(query, values)
        
    def create_Syndics_Tags(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS Syndics_Tags (
                SIRET_Syndic VARCHAR(14),
                ID_Tag VARCHAR(4),
                FOREIGN KEY (SIRET_Syndic) REFERENCES Syndics(SIRET),
                FOREIGN KEY (ID_Tag) REFERENCES Tags(ID)
            )
            '''
        self.create_table(create_table_query)
    
    def add_data_Syndics_Tags(self, syndics_tags_path):
        syndics_tags_df = pd.read_csv(syndics_tags_path)
        syndics_tags_df.fillna('', inplace=True)

        for index, row in syndics_tags_df.iterrows():
            #Make a get request to check if the SIRET_Syndic and ID_Tag already exists in the database
            query = "SELECT * FROM Syndics_Tags WHERE SIRET_Syndic = %s AND ID_Tag = %s"
            values = (row['SIRET_Syndic'], row['ID_Service'])
            res = mysql.select(query, values)
            
            #Check if empty result
            if res == () :  #Empty --> add row
                query = '''
                    INSERT INTO `Syndics_Tags` 
                    (`SIRET_Syndic`, `ID_Tag`)
                    VALUES (%s, %s)
                '''
                values = (row['SIRET_Syndic'], row['ID_Service'])
                mysql.insert(query, values)
            
            else : #Not empty --> update row
                query = '''
                    UPDATE `Syndics_Tags` 
                    SET `SIRET_Syndic` = %s, `ID_Tag` = %s
                    WHERE `SIRET_Syndic` = %s AND `ID_Tag` = %s
                '''
                values = (row['SIRET_Syndic'], row['ID_Service'], row['SIRET_Syndic'], row['ID_Service'])
                mysql.update(query, values)
    
    def create_Horaires(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS Horaires (
                SIRET_Syndic VARCHAR(14),
                Jour_Semaine VARCHAR(12),
                Heure_Ouverture TIME,
                Heure_Fermeture TIME,
                FOREIGN KEY (SIRET_Syndic) REFERENCES Syndics(SIRET)
            )
            '''
        self.create_table(create_table_query)

    def add_data_Horaires(self, horaires_path):
        horaires_df = pd.read_csv(horaires_path)
        horaires_df.fillna('', inplace=True)

        for index, row in horaires_df.iterrows():
            #Make a get request to check if the SIRET_Syndic and Jour_Semaine already exists in the database
            query = "SELECT * FROM Horaires WHERE SIRET_Syndic = %s AND Jour_Semaine = %s"
            values = (row['SIRET_Syndic'], row['Jour_Semaine'])
            res = mysql.select(query, values)
            
            #Check if empty result
            if res == () : #Empty --> add row
                query = '''
                    INSERT INTO `Horaires` 
                    (`SIRET_Syndic`, `Jour_Semaine`, `Heure_Ouverture`, `Heure_Fermeture`)
                    VALUES (%s, %s, %s, %s)
                '''
                values = (row['SIRET_Syndic'], row['Jour_Semaine'], row['Heure_Ouverture'], row['Heure_Fermeture'])
                mysql.insert(query, values)
            
            else : #Not empty --> update row
                query = '''
                    UPDATE `Horaires` 
                    SET `SIRET_Syndic` = %s, `Jour_Semaine` = %s, `Heure_Ouverture` = %s, `Heure_Fermeture` = %s
                    WHERE `SIRET_Syndic` = %s AND `Jour_Semaine` = %s
                '''
                values = (row['SIRET_Syndic'], row['Jour_Semaine'], row['Heure_Ouverture'], row['Heure_Fermeture'], row['SIRET_Syndic'], row['Jour_Semaine'])
                mysql.update(query, values)
            
host = "reve-syndics.cvkw4s2kg4nz.eu-north-1.rds.amazonaws.com"
user = "admin"
password = "BonjourReVEsolution2023!"
db = "reve_syndic_db"

syndics_path = 'data/clean/merged_syndics.csv'
copros_path = 'data/clean/copros.csv'
tags_path = 'data/clean/tags.csv'
syndics_tags_path = 'data/clean/tags_syndics.csv'
horaires_path = 'data/clean/horaires_syndics.csv'

mysql = MySQL(host, user, password, db)
try:
    mysql.connect()

    # --------- Create Table Syndics ------------
    #mysql.create_Syndics()

    # --------- Add data to Syndics  ------------
    #mysql.add_data_Syndics(syndics_path)
    #print("Syndics OK")

    # -------------- Create Table Copros ------------
    #mysql.create_Copros()

    # -------------- Add data to Copros ------------
    #mysql.add_data_Copros(copros_path)
    #print("Copros OK")

    # -------------- Create Table Tags ------------
    mysql.create_Tags()

    # -------------- Add data to Tags ------------
    mysql.add_data_Tags(tags_path)

    # -------------- Create Table Syndics_Tags ------------
    mysql.create_Syndics_Tags()

    # -------------- Add data to Syndics_Tags ------------
    mysql.add_data_Syndics_Tags(syndics_tags_path)

    # -------------- Create Table Horaires ------------
    mysql.create_Horaires()

    # -------------- Add data to Horaires ------------
    mysql.add_data_Horaires(horaires_path)

            
except pymysql.Error as e:
    print(f"Error: {e}")
finally:
    mysql.close()
