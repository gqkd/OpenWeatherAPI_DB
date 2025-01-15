CREATE SCHEMA OpenWeatherAPI;

CREATE TABLE LK_WEATHER_TYPE (
    ID_WEATHER_TYPE INT(3) NOT NULL PRIMARY KEY,
    WEATHER_TYPE_DS NVARCHAR(50),
    WEATHER_TYPE_LDS NVARCHAR(100)
);

INSERT INTO LK_WEATHER_TYPE (ID_WEATHER_TYPE, WEATHER_TYPE_DS, WEATHER_TYPE_LDS) 
VALUES 
(200, 'Thunderstorm', 'thunderstorm with light rain'),
(201, 'Thunderstorm', 'thunderstorm with rain'),
(202, 'Thunderstorm', 'thunderstorm with heavy rain'),
(210, 'Thunderstorm', 'light thunderstorm'),
(211, 'Thunderstorm', 'thunderstorm'),
(212, 'Thunderstorm', 'heavy thunderstorm'),
(221, 'Thunderstorm', 'ragged thunderstorm'),
(230, 'Thunderstorm', 'thunderstorm with light drizzle'),
(231, 'Thunderstorm', 'thunderstorm with drizzle'),
(232, 'Thunderstorm', 'thunderstorm with heavy drizzle'),
(300, 'Drizzle', 'light intensity drizzle'),
(301, 'Drizzle', 'drizzle'),
(302, 'Drizzle', 'heavy intensity drizzle'),
(310, 'Drizzle', 'light intensity drizzle rain'),
(311, 'Drizzle', 'drizzle rain'),
(312, 'Drizzle', 'heavy intensity drizzle rain'),
(313, 'Drizzle', 'shower rain and drizzle'),
(314, 'Drizzle', 'heavy shower rain and drizzle'),
(321, 'Drizzle', 'shower drizzle'),
(500, 'Rain', 'light rain'),
(501, 'Rain', 'moderate rain'),
(502, 'Rain', 'heavy intensity rain'),
(503, 'Rain', 'very heavy rain'),
(504, 'Rain', 'extreme rain'),
(511, 'Rain', 'freezing rain'),
(520, 'Rain', 'light intensity shower rain'),
(521, 'Rain', 'shower rain'),
(522, 'Rain', 'heavy intensity shower rain'),
(531, 'Rain', 'ragged shower rain'),
(600, 'Snow', 'light snow'),
(601, 'Snow', 'snow'),
(602, 'Snow', 'heavy snow'),
(611, 'Snow', 'sleet'),
(612, 'Snow', 'light shower sleet'),
(613, 'Snow', 'shower sleet'),
(615, 'Snow', 'light rain and snow'),
(616, 'Snow', 'rain and snow'),
(620, 'Snow', 'light shower snow'),
(621, 'Snow', 'shower snow'),
(622, 'Snow', 'heavy shower snow'),
(701, 'Mist', 'mist'),
(711, 'Smoke', 'smoke'),
(721, 'Haze', 'haze'),
(731, 'Dust', 'sand/dust whirls'),
(741, 'Fog', 'fog'),
(751, 'Sand', 'sand'),
(761, 'Dust', 'dust'),
(762, 'Ash', 'volcanic ash'),
(771, 'Squall', 'squalls'),
(781, 'Tornado', 'tornado'),
(800, 'Clear', 'clear sky'),
(801, 'Clouds', 'few clouds: 11-25%'),
(802, 'Clouds', 'scattered clouds: 25-50%'),
(803, 'Clouds', 'broken clouds: 51-84%'),
(804, 'Clouds', 'overcast clouds: 85-100%')
;

CREATE TABLE LK_CITIES (
    ID_CITY INT(6) NOT NULL PRIMARY KEY,
    CITY_NAME NVARCHAR(50),
    LATITUDE NUMERIC(9,6),
    LONGITUDE NUMERIC(9,6),
    PROVINCE_CD NVARCHAR(5),
    PROVINCE_DS NVARCHAR(50),
    REGION_CD NVARCHAR(5),
    REGION_DS NVARCHAR(50),
    STATE_ALPHA_CD NVARCHAR(3),
    STATE_NUM_CD NUMERIC(3),
    STATE_DS NVARCHAR(50)
);


INSERT INTO LK_CITIES VALUES 
(001, 'MILANO', 44.494888, 11.342616, 'IT-MI','MILANO', 'IT-25','LOMBARDIA', 'ITA',380,'ITALIA'),
(002, 'BOLOGNA', 45.464203, 9.189982, 'IT-BO', 'BOLOGNA', 'IT-45','EMILIA-ROMAGNA','ITA',380,'ITALIA'),
(003, 'CAGLIARI', 39.223843, 9.121661, 'IT-CA', 'CAGLIARI', 'IT-88','SARDEGNA','ITA',380,'ITALIA');

DROP TABLE IF EXISTS FACT_WEATHER;
CREATE TABLE FACT_WEATHER (
	ID_OBSERVATION INT(10) NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ID_SOURCE NVARCHAR(50),
    ID_CITY INT(6),
    OBSERVATION_TS DATETIME,
    TEMPERATURE NUMERIC(4,2),
    ID_WEATHER_TYPE INT(3),
    WIND_SPEED NUMERIC(5,2)
);