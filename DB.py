import mysql.connector, datetime
from mysql.connector import errorcode

CONFIG = {
    'user': 'root',
    'password': 'MOT DE PASSE',
    'host': '127.0.0.1',
    'database': 'velibs',
    'raise_on_warnings': True
}

def testDatabase():
    ################### DATABASE TEST ###################
    try:
        cnx = mysql.connector.connect(**CONFIG)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Quelque chose ne va pas avec le nom d'utilisateur et/ou le mot de passe du compte de la BDD.")
            exit()
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("La base de données n'existe pas")
            exit()
        else:
            print("Erreur : " + err)
            exit()
    else:
        print("DATABASE OK")
        cnx.close()
    #####################################################

class DB:
    def __init__(self):
        self.mydb = mysql.connector.connect(**CONFIG)
        self.cursor = self.mydb.cursor()

    def init_velibs_tables(self):
        # Table statique
        sql = "CREATE TABLE station_information (stationcode TEXT, name TEXT, capacity INTEGER, coordonnees_geo TEXT, PRIMARY KEY (stationcode(20)))"
        try:
            self.cursor.execute(sql)
        except mysql.connector.Error as e:
            print(e)
        
        # Table dynamique
        sql = "CREATE TABLE station_status (stationcode TEXT, is_installed TEXT, numdocksavailable INTEGER, numbikesavailable INTEGER, mechanical INTEGER, ebike INTEGER, nom_arrondissement_communes TEXT, datetime DATETIME)"
        try:
            self.cursor.execute(sql)
        except mysql.connector.Error as e:
            print(e)
        
        # Table history_changes
        sql = "CREATE TABLE history_change (datetime DATETIME, action TEXT, table_name TEXT)"
        try:
            self.cursor.execute(sql)
        except mysql.connector.Error as e:
            pass
        
    def update_station_information(self, name: str, capacity: int, coordonnees_geo: str, station_code: str):
        val = (str(name), str(capacity), str(coordonnees_geo), str(station_code),)
        sql = "UPDATE station_information SET name = %s, capacity = %s, coordonnees_geo = %s WHERE stationcode = %s"
        self.cursor.execute(sql, val)
        
    def insert_station_information(self, name: str, capacity: int, coordonnees_geo: str, station_code: str):
        val = (str(station_code), str(name), str(capacity), str(coordonnees_geo),)
        sql = "INSERT INTO station_information(stationcode, name, capacity, coordonnees_geo) VALUES (%s, %s, %s, %s)"
        self.cursor.execute(sql, val)
        
    def update_station_status(self, is_installed: str, numdocksavailable: int, numbikesavailable: int, mechanical: int, ebike: int, nom_arrondissement_communes: str, stationcode: str, datetime: datetime.datetime):
        val = (str(is_installed), str(numdocksavailable), str(numbikesavailable), str(mechanical), str(ebike), str(nom_arrondissement_communes), datetime, str(stationcode),)
        sql = "UPDATE station_status SET is_installed = %s, numdocksavailable = %s, numbikesavailable = %s, mechanical = %s, ebike = %s, nom_arrondissement_communes = %s, datetime = %s WHERE stationcode = %s"
        self.cursor.execute(sql, val)
        
    def insert_station_status(self, is_installed: str, numdocksavailable: int, numbikesavailable: int, mechanical: int, ebike: int, nom_arrondissement_communes: str, stationcode: str, datetime: datetime.datetime):
        val = (str(is_installed), str(numdocksavailable), str(numbikesavailable), str(mechanical), str(ebike), str(nom_arrondissement_communes), str(stationcode), datetime,)
        sql = "INSERT INTO station_status(is_installed, numdocksavailable, numbikesavailable, mechanical, ebike, nom_arrondissement_communes, stationcode, datetime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        self.cursor.execute(sql, val)
        
    def select_all_dates(self):
        sql = "SELECT DISTINCT datetime FROM station_status"
        self.cursor.execute(sql)
        
    def select_station_status_by_date(self, datetime: datetime.datetime):
        val = (datetime,)
        sql = "SELECT * FROM station_status WHERE datetime = %s"
        self.cursor.execute(sql, val)
        
    def select_all_stations_information(self):
        sql = "SELECT * FROM station_information"
        self.cursor.execute(sql)
        
    def createLogTriggers(self):
        # Vérifie si les triggers existe déjà
        sql = "DROP TRIGGER IF EXISTS trigger_insert_information"
        try:
            self.cursor.execute(sql)
        except mysql.connector.errors.DatabaseError:
            pass
        sql = "DROP TRIGGER IF EXISTS trigger_update_information"
        try:
            self.cursor.execute(sql)
        except mysql.connector.errors.DatabaseError:
            pass
        sql = "DROP TRIGGER IF EXISTS trigger_delete_information"
        try:
            self.cursor.execute(sql)
        except mysql.connector.errors.DatabaseError:
            pass
        sql = "DROP TRIGGER IF EXISTS trigger_insert_status"
        try:
            self.cursor.execute(sql)
        except mysql.connector.errors.DatabaseError:
            pass
        sql = "DROP TRIGGER IF EXISTS trigger_update_status"
        try:
            self.cursor.execute(sql)
        except mysql.connector.errors.DatabaseError:
            pass
        sql = "DROP TRIGGER IF EXISTS trigger_delete_status"
        try:
            self.cursor.execute(sql)
        except mysql.connector.errors.DatabaseError:
            pass
        
        sql = "CREATE TRIGGER trigger_insert_information AFTER INSERT ON station_information FOR EACH ROW BEGIN IF NOT EXISTS (SELECT 1 FROM history_change WHERE datetime = NOW()) THEN INSERT INTO history_change VALUES (NOW(),'INSERT','station_information');END IF;END;"
        self.cursor.execute(sql)
        
        sql = "CREATE TRIGGER trigger_update_information AFTER UPDATE ON station_information FOR EACH ROW BEGIN IF NOT EXISTS (SELECT 1 FROM history_change WHERE datetime = NOW()) THEN INSERT INTO history_change VALUES (NOW(),'UPDATE','station_information');END IF;END;"
        self.cursor.execute(sql)
        
        sql = "CREATE TRIGGER trigger_delete_information AFTER DELETE ON station_information FOR EACH ROW BEGIN IF NOT EXISTS (SELECT 1 FROM history_change WHERE datetime = NOW()) THEN INSERT INTO history_change VALUES (NOW(),'DELETE','station_information');END IF;END;"
        self.cursor.execute(sql)
        
        sql = "CREATE TRIGGER trigger_insert_status AFTER INSERT ON station_status FOR EACH ROW BEGIN IF NOT EXISTS (SELECT 1 FROM history_change WHERE datetime = NOW()) THEN INSERT INTO history_change VALUES (NOW(),'INSERT','station_status');END IF;END;"
        self.cursor.execute(sql)
        
        sql = "CREATE TRIGGER trigger_update_status AFTER UPDATE ON station_status FOR EACH ROW BEGIN IF NOT EXISTS (SELECT 1 FROM history_change WHERE datetime = NOW()) THEN INSERT INTO history_change VALUES (NOW(),'UPDATE','station_status');END IF;END;"
        self.cursor.execute(sql)
        
        sql = "CREATE TRIGGER trigger_delete_status AFTER DELETE ON station_status FOR EACH ROW BEGIN IF NOT EXISTS (SELECT 1 FROM history_change WHERE datetime = NOW()) THEN INSERT INTO history_change VALUES (NOW(),'DELETE','station_status');END IF;END;"
        self.cursor.execute(sql)
        
    def execute(self, sql):
        self.cursor.execute(sql)

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()
    
    def commit(self):
        self.mydb.commit()

    def close(self):
        self.mydb.close()