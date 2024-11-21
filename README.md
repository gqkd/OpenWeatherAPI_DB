# OpenWeatherAPI_DB
Automates fetching, storing, and analyzing hourly weather data from the OpenWeatherMap API. Includes MySQL database integration, raw data storage, and scripts for analysis using SQL and Python. Extensible for more cities or data sources, ideal for learning ETL pipelines, API integration, and data management.

## Overview

This project provides a comprehensive solution for fetching, storing, and analyzing weather data for multiple cities. Using the OpenWeatherMap API, we retrieve hourly weather conditions, store them in a MySQL database, and perform detailed analyses to answer various questions related to weather trends and statistics.

## Project Structure

The repository is organized as follows:

- `config.json`: Configuration file containing API key and database settings  
- `main.py`: Main script to fetch and store weather data  
- `utils.py`: Utility functions for API calls and database operations  
- `analysis.py`: Script for analyzing weather data stored in raw JSON files  
- `DDL.SQL`: SQL file to create and set up database schema  
- `QUERIES.SQL`: SQL file containing analytical queries  
- `raw_data`: Directory for storing raw weather data (JSON format) , automatically created

## Steps

### 1. Configure the Environment

Update the `config.json` file with your OpenWeatherMap API key and database credentials:
   ```json
   {
       "api_key": "<your_api_key>",
       "base_url": "https://api.openweathermap.org/data/3.0/onecall",
       "db_config": {
           "host": "localhost",
           "user": "<your_user>",
           "password": "<your_password>",
           "database": "openweatherapi"
       }
   }
  ```
### 2. Database Setup  
Open the `DDL.SQL` file and execute the script in your MySQL database to create the necessary tables:

```sql
CREATE SCHEMA OpenWeatherAPI;

CREATE TABLE LK_WEATHER_TYPE (...);
CREATE TABLE LK_CITIES (...);
CREATE TABLE FACT_WEATHER (...);
```
Insert lookup data for weather types and cities using the SQL provided in `DDL.SQL`.

### 3. Fetching and Storing Weather Data  
The `main.py` script automates the process of retrieving and storing weather data.

Define the cities to fetch weather data for in `main.py`:

```python
cities = ['Milano', 'Bologna', 'Cagliari']
```

Run the script to fetch and store weather data for the last 3 days:

```python
python main.py
```
The script:

- Retrieves data using the OpenWeatherMap API.  
- Saves raw JSON data in the `raw_data` directory.  
- Inserts cleaned data into the `FACT_WEATHER` table in the MySQL database.  

### 4. Analyzing Weather Data

#### Using SQL Queries  
Open the `QUERIES.SQL` file to find SQL queries for analysis, including:  

- Distinct weather conditions observed in a specific period.  
- Ranking of the most common weather conditions per city.  
- Average temperature observed per city.  
- City with the highest temperature.  
- City with the highest daily temperature variation.  
- City with the strongest wind.  

Execute these queries directly in your MySQL client to retrieve the analysis results.

#### Using Python (Pandas)  
Run `analysis.py` to perform data analysis directly on the raw JSON files. It answers the same questions as the SQL queries but uses pandas for processing.

```bash
python analysis.py
```
Example outputs include:

- Weather condition rankings per city.
- Temperature averages.
- Highest temperature and wind speed.
- Daily temperature variations.


---

### Contributions  

Feel free to submit pull requests for improvements or additional features. For major changes, please discuss them via issues first.  


