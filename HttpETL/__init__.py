import logging

import azure.functions as func

import azure.functions as func
import json    
from cassandra.auth import PlainTextAuthProvider
import urllib.request
from cassandra.query import BatchStatement, SimpleStatement
import datetime
import urllib.request

import time
import ssl
 
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE
from requests.utils import DEFAULT_CA_BUNDLE_PATH 
import os

#parametres de configuration à recupere à partir de cosmos bd API Cassandra
config = {
         'username': 'mycassandra',

         'password': '75liNIqWSkfH3YIRsdrh2eQJ7geFBNVlbIqyQRLPyO5KirmmZ9zoRPh9I7hZOEJeufA8M5HOe6SVbd3FywB9qw==',

         'contactPoint': 'mycassandra.cassandra.cosmos.azure.com',

         'port':'10350'
         }

# Créé une url
def url_builder(city_id,city_name,country):
    user_api = '986c1f2c3c5dc15f50d6c366bc3fc05f'  # Obtain yours form: http://openweathermap.org/
    unit = 'metric'  # For Fahrenheit use imperial, for Celsius use metric, and the default is Kelvin.
    if(city_name!=""):
        api = 'http://api.openweathermap.org/data/2.5/weather?q=' # "http://api.openweathermap.org/data/2.5/weather?q=Tunis,fr
        full_api_url = api + str(city_name) +','+ str(country)+ '&mode=json&units=' + unit + '&APPID=' + user_api
    else:
        api = 'http://api.openweathermap.org/data/2.5/weather?id='# Search for your city ID here: http://bulk.openweathermap.org/sample/city.list.json.gz
        full_api_url = api + str(city_id) + '&mode=json&units=' + unit + '&APPID=' + user_api
    return full_api_url

# Fait un appel à une API via une url et renvoie les données recues
def data_fetch(full_api_url):
    url = urllib.request.urlopen(full_api_url)
    output = url.read().decode('utf-8')
    raw_api_dict = json.loads(output)#convertir une chaine comme dictionnaire
    url.close()#fermer la connexion au serveur api
    return raw_api_dict

# Convertit une chaîne caractères en Timestamp
def time_converter(time):
    converted_time = datetime.datetime.fromtimestamp(int(time)).strftime('%I:%M %p')
    return converted_time

# Réorganise les données dans une structure précise
def data_organizer(raw_api_dict):
    data = dict(
        city=raw_api_dict.get('name'),
        country=raw_api_dict.get('sys').get('country'),
        temp=raw_api_dict.get('main').get('temp'),
        temp_max=raw_api_dict.get('main').get('temp_max'),
        temp_min=raw_api_dict.get('main').get('temp_min'),
        humidity=raw_api_dict.get('main').get('humidity'),
        pressure=raw_api_dict.get('main').get('pressure'),
        sky=raw_api_dict['weather'][0]['main'],
        sunrise=time_converter(raw_api_dict.get('sys').get('sunrise')),
        sunset=time_converter(raw_api_dict.get('sys').get('sunset')),
        wind=raw_api_dict.get('wind').get('speed'),
        wind_deg=raw_api_dict.get('deg'),
        dt=time_converter(raw_api_dict.get('dt')),
        cloudiness=raw_api_dict.get('clouds').get('all')
    )
    return data

 
# Lit le fichier contenant les villes et leurs identifiants OpenWeather et 
 
   
#############################################################################

 
#on force ici a repecter le datframe de pandas lors de la recuperation des données
##definir une fonction qui permet d'etablir une connexion et initialiser le schema du keypsace
def ConnectCassandra():
    #<authenticateAndConnect>
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.verify_mode = CERT_NONE
    auth_provider = PlainTextAuthProvider(username=config['username'], password=config['password'])
    cluster = Cluster([config['contactPoint']], port = config['port'], auth_provider=auth_provider,ssl_context=ssl_context)
    session = cluster.connect()
    #<createKeyspace>
    session.execute("CREATE KEYSPACE IF NOT EXISTS Weather "\
                "WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1};")

    session.execute("CREATE TABLE IF NOT EXISTS weather.cities_table (city TEXT,country TEXT,temp DOUBLE,temp_max DOUBLE ,"\
                "temp_min DOUBLE,humidity INT,pressure INT,sky TEXT,sunrise TEXT,sunset TEXT,wind DOUBLE,"\
                "dt TEXT,cloudiness INT,PRIMARY KEY (city));")
    #</authenticateAndConnect>
    return session 

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    print("Handling request to ETL Processing.")
    try:
        ##paremetre pays à selectionner leur villes
        country = req.params.get('country')
        #nombre maximale de ville à ingester
        print(req.params.get('city'))
        print(req.params.get('country'))
        city=req.params.get('city')
        ##creation d'un connexion
        data_to_str=json.dumps('No Data ')
        session=ConnectCassandra()
        print(session)
        
                
        url=url_builder('',city,country)
        #Invocation du API afin de recuperer les données
        data=data_fetch(url)
        #Formatage des données
        data_organized=data_organizer(data)
        # On supprime la clé "wind_deg" car sa valeur est toujours nulle
        if 'wind_deg' in data_organized.keys(): del data_organized["wind_deg"]
        
        # On caste en string le dictionnaire 
        data_to_str = json.dumps(data_organized,ensure_ascii=False)
        # On ajoute des "'" en début et fin de string pour respecter la syntaxe de la fonction JSON
        # On remplace également les "'" par des espaces pour éviter des problèmes de format
        data_to_str = "'"+data_to_str.replace("'"," ")+"'"
        # On effectue l'insertion dans la table avec une query CQL, en utilisant la fonction JSON
        session.execute("INSERT INTO weather.cities_table JSON {}".format(data_to_str)) 
        logging.info("INSERT INTO weather.cities_table JSON {}".format(data_to_str) ) 
        return func.HttpResponse("This HTTP triggered function executed successfully.  for  {}  {} .".format(city,country),status_code=200)
    except Exception as e:
        logging.error('HTTP triggered function has a problem issues {}.'.format( str(e)))
        return func.HttpResponse(
             "This HTTP triggered function has a problem issues {}.".format( str(e)),
             status_code=400
        )
    