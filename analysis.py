
import os
import json
import pandas as pd
from datetime import datetime, timezone

RAW_DATA_DIR = "raw_data"

def load_raw_data_to_dataframe(dir:str = RAW_DATA_DIR):
    # Inizializza lista per i dati
    data = []
    
    # Itera su tutti i file della cartella
    for filename in os.listdir(dir):
        with open(os.path.join(dir, filename), 'r') as file:
            
            raw_data = json.load(file)
            
            # Prende il nome della città dal file json (viene sempre salvato come Nomecitta_timestamp)
            city_name = filename.split('_')[0]
            observation_ts = datetime.fromtimestamp(raw_data['data'][0]['dt'], tz=timezone.utc)
            temperature = raw_data['data'][0]['temp']
            wind_speed = raw_data['data'][0]['wind_speed']
            
            # Itera sui weather presenti, ce ne possono essere più di uno
            for weather in raw_data['data'][0]['weather']:
                weather_type = weather['main']
                weather_type_ds = weather['description']
                    
                # Appende l'osservazione relativa a questo weather type
                data.append({
                    "city": city_name,
                    "observation_ts": observation_ts,
                    "temperature": temperature,
                    "wind_speed": wind_speed,
                    "weather_type": weather_type,
                    "weather_type_ds":weather_type_ds
                })

    df = pd.DataFrame(data)
    
    # Converte il timestamp in formato data
    df['observation_ts'] = pd.to_datetime(df['observation_ts'])
    
    print(df.head())
    return df

df = load_raw_data_to_dataframe()



#How many distinct weather conditions were observed (rain/snow/clear/…) in a certain period?
print(df['weather_type_ds'].nunique())


#############################Rank the most common weather conditions in a certain period of time per city#############################

# Salva in count il conteggio delle varie condizioni raggruppate per città
counts = df.groupby(['city', 'weather_type_ds']).size().reset_index(name='count')
print(counts.sort_values(['city','count'], ascending= [False,False]))

#############################What are the temperature averages observed in a certain period per city?#############################
print(df.groupby('city')['temperature'].mean())


#############################What city had the highest absolute temperature in a certain period of time?#############################
print(df.groupby('city')['temperature'].max())


#############################Which city had the highest daily temperature variation in a certain period of time?#############################
# Aggiunge una colonna per il giorno
df['data'] = df['observation_ts'].dt.date

# Nuovo dataframe dove salvo la temperatura max e min per giornata
variazione_giornaliera = df.groupby(['city', 'data'])['temperature'].agg(['min', 'max'])

#Aggiunge la colonna per la variazione giornaliera
variazione_giornaliera['variazione'] = variazione_giornaliera['max']-variazione_giornaliera['min']

# Trova il massimo e fa un sort
print(variazione_giornaliera.groupby(['city','data'])['variazione'].max().sort_values(ascending=False))


#############################What city had the strongest wind in a certain period of time?#############################
print(df.groupby('city')['wind_speed'].max().sort_values(ascending=False))