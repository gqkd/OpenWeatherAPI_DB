from utils import *

#Milano: lat 44.494888 long 11.342616
#Bologna: lat 45.464203 long 9.189982
#Cagliari: lat 39.223843 long 9.121661

if __name__ == "__main__":
    cities = ['Milano', 'Bologna', 'Cagliari']
    for city in cities:
        load_last_three_days(city)
