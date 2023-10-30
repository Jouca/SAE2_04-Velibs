import DB

import folium, json, time, datetime, geopandas, base64, threading, random, pandas, io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from datetime import timedelta
from shapely.geometry.polygon import Point
from shapely.ops import unary_union, cascaded_union
import matplotlib.pyplot as plt
import numpy as np

from flask import Flask, render_template, request, Markup

saved_map = []
app = Flask(__name__)


PERIODIC_THREAD_IDS = set()
class PeriodicThread(threading.Thread):
    def __init__(self, interval, target, name=None, on_shutdown=None):
        super(PeriodicThread, self).__init__(name=name)
        self._target = target
        self._on_shutdown = on_shutdown
        self.interval = interval
        self.quit = threading.Event()
        self.daemon = True

    def start(self):
        super(PeriodicThread, self).start()
        PERIODIC_THREAD_IDS.add(self.ident)

    def stop(self):
        self.quit.set()

    def run(self):
        try:
            while not self.quit.wait(self.interval):
                self._target()
            if self._on_shutdown is not None:
                self._on_shutdown()
                PERIODIC_THREAD_IDS.remove(self.ident)
        finally:
            PERIODIC_THREAD_IDS.remove(self.ident)


def createMarker(map, datas, time):
    markers = []
    for data in datas:
        info_bulle = ""
        
        if data["is_installed"] == "NON":
            color = "black"
            info_bulle = f"""
            <div>
                <img src="data:image/png;base64,{(base64.b64encode(open('./ressources/sprites/interdit.png', 'rb').read())).decode('UTF-8')}" width=15% style="vertical-align:middle">
                <span style="font-size: 13px; padding: 10px;"><b>Cette station est fermé</b></span>
            </div>
            """
        elif data["numbikesavailable"] == 0:
            color = "red"
            info_bulle = f"""
            <div>
                <img src="data:image/png;base64,{(base64.b64encode(open('./ressources/sprites/croix.png', 'rb').read())).decode('UTF-8')}" width=12% style="vertical-align:middle">
                <span style="font-size: 13px; padding: 13px;"><b>Aucun vélib disponible</b></span>
            </div>
            """
        else:
            color = "green"
            
        if time != -1:
            redirect_url = f'./station?station_id={data["stationcode"]}&time={time}'
        else:
            redirect_url = f'./station?station_id={data["stationcode"]}'
        
        popup = folium.Popup(f"""
            <center>
                <h3><u>{data["name"]}</u></h3>
                {info_bulle}
                <img src="data:image/png;base64,{(base64.b64encode(open('./ressources/sprites/velo_p.png', 'rb').read())).decode('UTF-8')}" width=20%>
                <br>
                <br>
                <span style="font-size: 15px; padding: 10px; line-height: 0px; text-size-adjust: auto; white-space: nowrap;">Stations vélibs disponibles : {data["numdocksavailable"]}</span>
                <br>
                <br>
                <br>
                <div>
                    <ul>
                        <li><div>
                            <img src="data:image/png;base64,{(base64.b64encode(open('./ressources/sprites/electric.png', 'rb').read())).decode('UTF-8')}" width=10% style="vertical-align:middle">
                            <span style="font-size: 13px; padding: 13px;">{data["ebike"]} vélib(s) électrique(s)</span>
                        </div></li>
                        <br>
                        <li><div>
                            <img src="data:image/png;base64,{(base64.b64encode(open('./ressources/sprites/mechanical.png', 'rb').read())).decode('UTF-8')}" width=10% style="vertical-align:middle">
                            <span style="font-size: 13px; padding: 13px;">{data["mechanical"]} vélib(s) mécanique(s)</span>
                        </div></li>
                    </ul>
                </div>
                <a href="{redirect_url}" style="font-size: 15px; text-decoration: none">Voir les statistiques</a>
                <p><b>Station code : {data["stationcode"]}</b></p>
            </center>
        """, max_width=700)
        
        markers.append(folium.Marker(
            location=[data["latitude"], data["longitude"]],
            popup=popup,
            icon=folium.Icon(
                icon="person-biking",
                prefix='fa',
                color=color
            ),
        ))
    
    for marker in markers:
        map.add_child(marker)
        
    return map


def generateMap(current_time=-1):
    # Initialisation de la BDD
    db = DB.DB()
    
    try:
        db.select_all_dates()
        dates = db.fetchall()
        
        if datetime.datetime.fromtimestamp(current_time) not in [date[0] for date in dates]:
            current_time = max(dates)[0]
            current_time = time.mktime(current_time.timetuple())
    except:
        pass
    
    try:
        db.select_station_status_by_date(datetime.datetime.fromtimestamp(current_time))
        datas_status = db.fetchall()
        db.select_all_stations_information()
        datas_stations = db.fetchall()
    except:
        pass
    
    db.close()
    
    datas = []
    
    map = folium.Map(
        location=[48.7784, 2.3847],
        width=800,
        height=600,
    )
    
    arrondissements = geopandas.read_file("./ressources/arrondissements.geojson")
    
    for i in range(1, 21):
        arrondissement = arrondissements[arrondissements["c_ar"] == i]
        
        # Compteur
        compteur_stations = 0
        compteur_velos = 0
        try:
            for station in datas_stations:
                if True in (arrondissement["geometry"].contains(Point(json.loads(station[3])["lon"], json.loads(station[3])["lat"]))).tolist():
                    compteur_stations += 1
                    for station_status in datas_status:
                        if station_status[0] == station[0]:
                            compteur_velos += station_status[3]
                            
            if current_time != -1:
                redirect_url = f"./render_map?ar={i}&time={current_time}"
            else:
                redirect_url = f"./render_map?ar={i}"
        
                
            popup = folium.Popup(f"""
                <center>
                    <h2>Arrondissement {i}</h2>
                    <div style="display: flex; align-items:center justify-content: center">
                        <div style="flex-basis: 40%">
                            <img src="data:image/png;base64,{(base64.b64encode(open('./ressources/sprites/position.png', 'rb').read())).decode('UTF-8')}" width=50%>
                        </div>
                        <div>
                            <p style="font-size: 13px;">{compteur_stations} stations</p>
                        </div>
                    </div>
                    <div style="display: flex; align-items:center justify-content: center">
                        <div style="flex-basis: 40%">
                            <img src="data:image/png;base64,{(base64.b64encode(open('./ressources/sprites/velo.png', 'rb').read())).decode('UTF-8')}" width=50%>
                        </div>
                        <div>
                            <p style="font-size: 12px;">{compteur_velos} vélibs disponibles</p>
                        </div>
                    </div>
                    <a href="{redirect_url}" style="font-size: 15px; text-decoration: none">Voir les stations</a>
                </center>
            """)
        except:
            popup = folium.Popup(f"""
                <center>
                    <h2>Arrondissement {i}</h2>
                </center>
            """)
        
        folium.GeoJson(data=arrondissement["geometry"].to_json(), style_function=lambda x: {'fillColor': 'orange'}, popup=popup).add_to(map, f"arrondissement{i}", i)

    datas = []
    
    try:
        for station in datas_stations:
            for status in datas_status:
                if station[0] == status[0]:
                    if True not in (arrondissements["geometry"].contains(Point(json.loads(station[3])["lon"], json.loads(station[3])["lat"]))).tolist():
                        if time.mktime(status[7].timetuple()) == current_time:
                            datas.append({
                                "stationcode": status[0],
                                "name": station[1],
                                "capacity": station[2],
                                "is_installed": status[1],
                                "numdocksavailable": status[2],
                                "numbikesavailable": status[3],
                                "mechanical": status[4],
                                "ebike": status[5],
                                "nom_arrondissement_communes": status[6],
                                "longitude": json.loads(station[3])["lon"],
                                "latitude": json.loads(station[3])["lat"],
                            })
    except:
        pass
    map = createMarker(map, datas, current_time)
        
    map.fit_bounds(map.get_bounds(), padding=(30, 30))
    
    saved_map.append((map, current_time))
    
    print("map generated")
    
    
def plot_png(fig):
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return output
    
    
def statistic_numbers_of_bikes(stations):
    fig, axs = plt.subplots()
    
    axs.plot(
        [datetime.datetime.strftime(date["date"], '%H:%M:%S') for date in stations],
        [int(ebike["ebike"]) for ebike in stations],
        color="cyan",
        label="Électrique"
    )
    axs.plot(
        [datetime.datetime.strftime(date["date"], '%H:%M:%S') for date in stations],
        [int(mechanical["mechanical"]) for mechanical in stations],
        color="gray",
        label="Mécanique"
    )
    axs.set_xlabel('Temps (Enregistré)')
    axs.set_ylabel('Nombre de vélo')
    axs.tick_params(axis='x', labelrotation = 60)
    axs.grid(True)
    axs.set_title(f"Nombre de vélos électrique/mécanique dans la station\n{stations[-1]['name']}\n depuis les dernières 24h", loc="right")
    
    fig.legend(loc="upper left")
    fig.tight_layout()
    
    return plot_png(fig)


def statistic_numbers_of_available_bikes(stations):
    fig, axs = plt.subplots()
    
    axs.plot(
        [datetime.datetime.strftime(date["date"], '%H:%M:%S') for date in stations],
        [int(ebike["numbikesavailable"]) for ebike in stations],
        color="green",
        label="NB vélos"
    )
    axs.plot(
        [datetime.datetime.strftime(date["date"], '%H:%M:%S') for date in stations],
        [int(ebike["numdocksavailable"]) for ebike in stations],
        color="blue",
        label="NB place de vélos"
    )
    axs.plot(
        [datetime.datetime.strftime(date["date"], '%H:%M:%S') for date in stations],
        [int(ebike["capacity"]) for ebike in stations],
        color="red",
        label="Capacité max"
    )
    
    axs.set_xlabel('Temps (Enregistré)')
    axs.set_ylabel('Nombre de vélo/place')
    axs.tick_params(axis='x', labelrotation = 60)
    axs.grid(True)
    axs.set_title(f"Nombre de vélos/place dans la station\n{stations[-1]['name']}\n depuis les dernières 24h", loc="right")
    
    fig.legend(loc="upper left")
    fig.tight_layout()
    
    return plot_png(fig)


@app.route("/station", methods=["GET"])
def station():
    # Vérification du fonctionnement de la base de données
    DB.testDatabase()

    station_id = request.args.get('station_id', default = 0, type = int)
    current_time = request.args.get('time', default = 0, type = int)
    
    # Initialisation de la BDD
    db = DB.DB()
    
    velib_electric_logo = (base64.b64encode(open('./ressources/sprites/electric.png', 'rb').read())).decode('UTF-8')
    velib_mecanic_logo = (base64.b64encode(open('./ressources/sprites/mechanical.png', 'rb').read())).decode('UTF-8')
    
    try:
        db.select_all_dates()
        dates = db.fetchall()

        if datetime.datetime.fromtimestamp(current_time) not in [date[0] for date in dates]:
            current_time = max(dates)[0]
            current_time = time.mktime(current_time.timetuple())
    except:
        pass

    try:
        datas_status = []
        for i in range(-5, 5):
            db.select_station_status_by_date(datetime.datetime.fromtimestamp(current_time + i))
            datas = db.fetchall()
            if datas != []:
                for data in datas:
                    datas_status.append(data)
        
        db.select_all_stations_information()
        datas_stations = db.fetchall()
    except:
        pass
    
    somme_capacite = 0
    somme_disponibilite_velo = 0
    somme_disponibilite_place = 0
    somme_ouverture = 0
    nb_stations = 0

    if station_id > 0:
        station_info = {}
        for station in datas_stations:
            for status in datas_status:
                if station[0] == status[0]:
                    if int(station[0]) == station_id:
                        station_info = {
                            "stationcode": status[0],
                            "name": station[1],
                            "capacity": station[2],
                            "is_installed": status[1],
                            "numdocksavailable": status[2],
                            "numbikesavailable": status[3],
                            "mechanical": status[4],
                            "ebike": status[5],
                            "nom_arrondissement_communes": status[6],
                            "longitude": json.loads(station[3])["lon"],
                            "latitude": json.loads(station[3])["lat"],
                        }
                        break
        if len(station_info.keys()) > 0:
            for status in datas_status:
                if station_info["stationcode"] == status[0]:
                    if int(station_info["stationcode"]) == station_id:
                        somme_capacite += int(station_info["capacity"])
                        somme_disponibilite_velo += int(status[3])
                        somme_disponibilite_place += int(status[2])
                        
                        if status[1] == "OUI":
                            somme_ouverture += 1
                            
                        nb_stations += 1
            
            date_list = [datetime_date for datetime_date in pandas.date_range(
                datetime.datetime.fromtimestamp(current_time) - timedelta(days=1),
                datetime.datetime.fromtimestamp(current_time),
                freq='S'
            ).to_pydatetime() if datetime_date in [date_listed[0] for date_listed in dates]]
            
            station_infos_list = []
            
            for date in date_list:
                db.select_station_status_by_date(date)
                datas_status = db.fetchall()
                
                # Récupération des stations pour les données de la journée
                for status in datas_status:
                    if station_info["stationcode"] == status[0]:
                        if int(station_info["stationcode"]) == station_id:
                            if date == status[7]:
                                station_infos_list.append({
                                    "stationcode": status[0],
                                    "name": station_info["name"],
                                    "capacity": station_info["capacity"],
                                    "is_installed": status[1],
                                    "numdocksavailable": status[2],
                                    "numbikesavailable": status[3],
                                    "mechanical": status[4],
                                    "ebike": status[5],
                                    "nom_arrondissement_communes": status[6],
                                    "longitude": station_info["longitude"],
                                    "latitude": station_info["latitude"],
                                    "date": date
                                })
                            
            figure_bikes = base64.b64encode(statistic_numbers_of_bikes(station_infos_list).getvalue()).decode('utf-8')
            figure_available_bikes_place = base64.b64encode(statistic_numbers_of_available_bikes(station_infos_list).getvalue()).decode('utf-8')
            
            
            if station_info["is_installed"] == "NON":
                info_bulle = f"""
                <div>
                    <img src="data:image/png;base64,{(base64.b64encode(open('./ressources/sprites/interdit.png', 'rb').read())).decode('UTF-8')}" width=3% style="vertical-align:middle">
                    <span style="font-size: 20px; padding: 10px;"><b>Cette station est fermé</b></span>
                </div>
                """
            elif station_info["numbikesavailable"] == 0:
                info_bulle = f"""
                <div>
                    <img src="data:image/png;base64,{(base64.b64encode(open('./ressources/sprites/croix.png', 'rb').read())).decode('UTF-8')}" width=3% style="vertical-align:middle">
                    <span style="font-size: 20px; padding: 13px;"><b>Aucun vélib disponible</b></span>
                </div>
                """
            else:
                info_bulle = ""
    
            svg_logo = open(f'./static/logo/Velibs_Stations_logo-0{random.randint(1, 5)}.svg').read()
            
            db.close()
            
            try:
                taux_disponibilite_velo = str(round((100*somme_disponibilite_velo) / somme_capacite, 3))
            except ZeroDivisionError:
                taux_disponibilite_velo = 0
            
            try:
                taux_disponibilite_place = str(round((100*somme_disponibilite_place) / somme_capacite, 3))
            except ZeroDivisionError:
                taux_disponibilite_place = 0
                
            try:
                taux_ouverture = str(round((100*somme_ouverture) / nb_stations, 3))
            except ZeroDivisionError:
                taux_ouverture = 0
                
            
            return render_template(
                "station.html",
                svg_logo=Markup(svg_logo),
                data=station_info,
                velib_electric_logo=velib_electric_logo,
                velib_mecanic_logo=velib_mecanic_logo,
                info_bulle=info_bulle,
                figure_bikes=figure_bikes,
                figure_available_bikes_and_place=figure_available_bikes_place,
                taux_disponibilite_velo=taux_disponibilite_velo,
                taux_disponibilite_place=taux_disponibilite_place,
                taux_ouverture=taux_ouverture
            )
    db.close()
    
    return "Station non trouvée"


@app.route('/render_map', methods=["GET"])
def render_map():
    # Vérification du fonctionnement de la base de données
    DB.testDatabase()
    
    current_time = request.args.get('time', default = 0, type = int)
    arrondissement = request.args.get('ar', default = 0, type = int)
    
    # Initialisation de la BDD
    db = DB.DB()
    
    try:
        db.select_all_dates()
        dates = db.fetchall()
        
        if datetime.datetime.fromtimestamp(current_time) not in [date[0] for date in dates]:
            current_time = max(dates)[0]
            current_time = time.mktime(current_time.timetuple())
            
            if saved_map[-1][1] != current_time:
                saved_map.pop(0)
                generateMap(current_time)
        else:
            saved_map.pop(0)
            generateMap(current_time)
    except:
        pass
    
    try:
        db.select_station_status_by_date(datetime.datetime.fromtimestamp(current_time))
        datas_status = db.fetchall()
        db.select_all_stations_information()
        datas_stations = db.fetchall()
    except:
        pass
    
    db.close()
    
    datas = []
    
    m = folium.Map(
        location=[48.7784, 2.3847],
        width=800,
        height=600,
    )
    
    arrondissements = geopandas.read_file("./ressources/arrondissements.geojson")
    
    if arrondissement > 0:
        arrondissement_polygon = arrondissements[arrondissements["c_ar"] == arrondissement]["geometry"]
        try:
            for station in datas_stations:
                for status in datas_status:
                    if station[0] == status[0]:
                        if True in (arrondissement_polygon.contains(Point(json.loads(station[3])["lon"], json.loads(station[3])["lat"]))).tolist():
                            if time.mktime(status[7].timetuple()) == current_time:
                                datas.append({
                                    "stationcode": status[0],
                                    "name": station[1],
                                    "capacity": station[2],
                                    "is_installed": status[1],
                                    "numdocksavailable": status[2],
                                    "numbikesavailable": status[3],
                                    "mechanical": status[4],
                                    "ebike": status[5],
                                    "nom_arrondissement_communes": status[6],
                                    "longitude": json.loads(station[3])["lon"],
                                    "latitude": json.loads(station[3])["lat"],
                                })
        except:
            pass
        
        m = createMarker(m, datas, current_time)
    else:
        if len(saved_map) > 0:
            saved_map[-1][0].fit_bounds(saved_map[-1][0].get_bounds(), padding=(30, 30))
            
            saved_map[-1][0].get_root().render()
            header = saved_map[-1][0].get_root().header.render()
            body_html = saved_map[-1][0].get_root().html.render()
            script = saved_map[-1][0].get_root().script.render()
            
            all_dates = [(date[0], int(time.mktime(date[0].timetuple()))) for date in dates]
            
            svg_logo = open(f'./static/logo/Velibs_Stations_logo-0{random.randint(1, 5)}.svg').read()
            return render_template("render_map.html", header=header, body_html=body_html, script=script, date=datetime.datetime.fromtimestamp(current_time), svg_logo=Markup(svg_logo), all_dates=all_dates)
    
    m.fit_bounds(m.get_bounds(), padding=(30, 30))
           
    m.get_root().render()
    header = m.get_root().header.render()
    body_html = m.get_root().html.render()
    script = m.get_root().script.render()
    
    all_dates = [(date[0], int(time.mktime(date[0].timetuple()))) for date in dates]
    
    svg_logo = open(f'./static/logo/Velibs_Stations_logo-0{random.randint(1, 5)}.svg').read()
    return render_template("render_map.html", header=header, body_html=body_html, script=script, svg_logo=Markup(svg_logo), all_dates=all_dates)

@app.route('/')
def hello():
    svg_logo = open(f'./static/logo/Velibs_Stations_logo-0{random.randint(1, 5)}.svg').read()
    return render_template("accueil.html", svg_logo=Markup(svg_logo))

def run_app():
    app.run(host="0.0.0.0", port=6937, threaded=True)

if __name__ == "__main__":
    generateMap()
    first_thread = threading.Thread(target=run_app)
    second_thread = PeriodicThread(60 * 2, generateMap)
    first_thread.start()
    second_thread.start()