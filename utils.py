import requests
import json
from datetime import datetime, timedelta, timezone
import mysql.connector
import os

with open("config.json", "r") as f:
    config = json.load(f)

API_KEY = config["api_key"]
BASE_URL = config["base_url"]
DB_CONFIG = config["db_config"]
TIME_MACHINE_URL = f'{BASE_URL}/timemachine'
RAW_DATA_DIR = "raw_data"

def db_connect(DB_CONFIG):
    #Connessione a MySQL
    try:
        db_connection = mysql.connector.connect(**DB_CONFIG)
        if db_connection.is_connected():
            print("Connesso a MySQL Database")
        return db_connection
    except mysql.connector.Error as e:
        print(f"Errore connessione a MySQL: {e}")
        return None

def db_close_connection(db_connection):
    if db_connection.is_connected():
        db_connection.close()
        print("Connessione a MySQL chiusa")
    else:
        print('Non connesso')
        
def generate_timestamps(delta_days: int =30) -> list:
    #TODO si potrebbe adattare per fare da delta1 a delta2
    # Data corrente
    adesso = datetime.now(timezone.utc)
    
    if adesso.minute != 0 or adesso.second != 0:
        adesso = adesso.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    
    # Data un delta fa
    delta_fa = adesso - timedelta(days=delta_days)
    timestamps = []
    
    while delta_fa < adesso:
        # Conversione in unix e append alla lista
        timestamps.append(int(delta_fa.timestamp()))
        # Scorre un'ora alla volta
        delta_fa += timedelta(hours=1)
        
    return timestamps

def get_weather_OpenWeatherAPI(lat:float, lon:float, timestamp) -> json:
    # Params da passare alla GET
    params = {
        'lat': lat,
        'lon': lon,
        'dt': timestamp,
        'appid': API_KEY,
        'units': 'metric'
    }
    
    try:
        response = requests.get(TIME_MACHINE_URL, params=params)
        response.raise_for_status()
        return response.json()
    
    except requests.RequestException as e:
        print(f"API request error: {e}")
        return None

def db_insert_one(db_connection, ID_SOURCE:str , ID_CITY:int , OBSERVATION_TS:str, TEMPERATURE:int, ID_WEATHER_TYPE:int, WIND_SPEED:int):
    try:
        # Istanza cursore per scrittura db
        cursor = db_connection.cursor()
        
        # Statement per l'insert
        stmt = """
        INSERT INTO FACT_WEATHER 
        (ID_SOURCE, ID_CITY, OBSERVATION_TS, TEMPERATURE, ID_WEATHER_TYPE, WIND_SPEED)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(stmt, (ID_SOURCE, ID_CITY, OBSERVATION_TS, TEMPERATURE, ID_WEATHER_TYPE, WIND_SPEED))
        db_connection.commit()

    except mysql.connector.Error as e:
        print(f"Errore insert: {e}")
    
    finally:
        cursor.close()


def get_city_info(db_connection, city_name: str):
    # Fa una query a db per prendere l'id della città, lat e long
    try:
        cursor = db_connection.cursor()
        query = "SELECT ID_CITY, LATITUDE, LONGITUDE  FROM LK_CITIES WHERE CITY_NAME = %s"
        cursor.execute(query, (city_name,))
        result = cursor.fetchone()
        
        if result:
            city_id, lat, lon = result
            return city_id, lat, lon 
        else:
            print(f"Citta {city_name} non presente in LK_CITIES.")
            return None
            
    except mysql.connector.Error as e:
        print(f"Errore query db: {e}")
        return
    
    finally:
        cursor.close()

def db_insert_multiple(past_n_days: int, city_name:str, id_source:str = "OpenWeatherAPI"):
    
    # Apro la connessione con i parametri di db
    db_connection = db_connect(DB_CONFIG)
    if not db_connection:
        print('Connessione non riuscita')
        return
    
    # Prendo le info della città di cui prendere i dati
    city_id, lat, lon = get_city_info(db_connection, city_name)

    # Genero i timestamp da iterare
    timestamps = generate_timestamps(past_n_days)

    for timestamp in timestamps:
        if id_source == "OpenWeatherAPI":
            data = get_weather_OpenWeatherAPI(lat, lon, timestamp)
            if not data:
                print(f'Errore: città {city_name} timestamp {timestamp} ')
                continue
            save_raw_data(data, city_name, timestamp)
            
        else:
            print('Source non presente')
            return None
        
        # Conversione del timestamp in formato per il db YYYY-MM-DD HH:MM:SS
        observation_ts = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        temperature = data['data'][0]['temp']
        wind_speed = data['data'][0]['wind_speed']
        
        # Itero sul numero di weather presenti (stessa città con timestamp può avere più di una condizione meteo)
        for weather in data['data'][0]['weather']:
            weather_type_id = weather['id']
            db_insert_one(db_connection, id_source, city_id, observation_ts, temperature, weather_type_id, wind_speed)
            print(f"Inserita osservazione per {city_name} alle {observation_ts}")

    db_close_connection(db_connection)

def save_raw_data(data: dict, city_name: str, timestamp: int):
    # Crea la cartella
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    
    filename = f"{RAW_DATA_DIR}/{city_name}_{timestamp}.json"
    
    # Scrive il file json
    with open(filename, "w") as f:
        json.dump(data, f)
    print(f"Dati salvati in {filename}")

def load_last_month_data(city):
    db_insert_multiple(30, city)

def load_last_three_days(city):
    db_insert_multiple(3,city)
    