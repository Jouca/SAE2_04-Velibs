import DB
from constants import API_LOCALISATION_VELIB, API_TEMPS_REEL_DISPONIBILITE, TIME_REFRESH_DATA_MINUTES

import pandas as pd
import datetime, asyncio, json
from dateutil import tz

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def getJSONData(url: str) -> pd.DataFrame:
    return pd.read_json(url, encoding="utf8")

def cleanJSON(df: pd.DataFrame) -> pd.DataFrame:
    df.drop(["name", "capacity", "is_renting", "is_returning", "duedate", "coordonnees_geo", "code_insee_commune"], axis = 1, inplace=True, errors='ignore')
    return df
        
        
def updateStaticTable():
    station_informations = getJSONData(API_LOCALISATION_VELIB)
    
    db = DB.DB()
    
    # Actualisation des données pour la table station_information
    for index in range(len(station_informations)):
        row = station_informations.iloc[index]
        try:
            db.insert_station_information(row["name"], int(row["capacity"]), json.dumps(row["coordonnees_geo"]), str(row["stationcode"]))
        except:
            db.update_station_information(row["name"], int(row["capacity"]), json.dumps(row["coordonnees_geo"]), str(row["stationcode"]))
    db.commit()
    db.close()
    print("Table station_information (table statique) mis à jour.")


async def updateDataTables():
    try:
        timezone = tz.gettz('Europe/Paris')
        
        while True:
            print(f"[{datetime.datetime.now().astimezone(tz=timezone).strftime('%Y-%m-%d %H:%M:%S')}] Mise à jour des données dynamiques...")
            
            # Obtention des data JSON
            station_statuses = cleanJSON(getJSONData(API_TEMPS_REEL_DISPONIBILITE))
            
            formatted_date = datetime.datetime.now().astimezone(tz=timezone).strftime('%Y-%m-%d %H:%M:%S')
            
            db = DB.DB()
            
            # Actualisation des données pour la table station_status
            for index in range(len(station_statuses)):
                row = station_statuses.iloc[index]
                try:
                    db.insert_station_status(row["is_installed"], int(row["numdocksavailable"]), int(row["numbikesavailable"]), int(row["mechanical"]), int(row["ebike"]), row["nom_arrondissement_communes"], str(row["stationcode"]), formatted_date)
                except:
                    pass
            db.commit()
            db.close()
            
            print(f"[{datetime.datetime.now().astimezone(tz=timezone).strftime('%Y-%m-%d %H:%M:%S')}] Données dynamiques mis à jour !")
        
            await asyncio.sleep(60 * TIME_REFRESH_DATA_MINUTES)
    except Exception as e:
        print("Erreur : ", e)


def inits():
    # Vérification du fonctionnement de la base de données
    DB.testDatabase()
    
    # Initialisation des tables
    db = DB.DB()

    db.init_velibs_tables()
    db.commit()
        
    db.createLogTriggers()
    db.commit()
    
    db.select_all_stations_information()
    static = db.fetchall()
    if len(static) == 0:
        # Initialisation des données statiques
        updateStaticTable()
    
    db.close()
    

if __name__ == "__main__":
    # Initialisations
    inits()
    # Démarrage de la tâche et de mettre à jour la table statique
    asyncio.run(updateDataTables())