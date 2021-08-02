## IMPORT LIBRARIES ##

import requests
import time
from datetime import date

from numpy import random
import pandas as pd

from pymongo import MongoClient

## DEFINE CONSTANTS ##

LIMIT = 50
TM_MIN, TM_MAX = 5, 9
YEAR_MONTH = date.today().strftime("%Y-%m")

## CREATE DICTIONARY CONTAINING INFORMATION OF CATEGORIES FOR EXTRACTING DATA ##

category_dict = {'id': 'MLC157522', 'property': 'apartment', 'operation': 'sale'}

# ID to select properties belonging to "Region Metropolitana"
STATE_ID = 'TUxDUE1FVEExM2JlYg'

## DEFINE FUNCTION TO MAKE REQUEST TO API ##

def extract_data_from_api(category_id:str=category_dict['id'], 
                          state_id:str=STATE_ID, 
                          city_id:str=None, 
                          neighborhood_id:str=None, 
                          bd_range:str=None, 
                          limit:int=None, 
                          offset:int=None,
                          total:bool=False, 
                          results:bool=False):
    
    base_url = 'https://api.mercadolibre.com/sites/MLC/search?'
    category = '&category={}'.format(category_id)
    state = '&state={}'.format(state_id)
    
    if city_id is None:
        city = ''
    else:
        city = '&city={}'.format(city_id)
        
    if neighborhood_id is None:
        neighborhood = ''
    else:
        neighborhood = '&neighborhood={}'.format(neighborhood_id)
        
    if bd_range is None:
        bedrooms = ''
    else:
        bedrooms = '&BEDROOMS={}'.format(bd_range)
        
    if limit is None:
        limit_url = ''
    else:
        limit_url = '&limit={}'.format(limit)
        
    if offset is None:
        offset_url = ''
    else:
        offset_url = '&offset={}'.format(offset)
    
    url = base_url + category + state + city + neighborhood + bedrooms + limit_url + offset_url
    response = requests.get(url)
    response_json = response.json()
    
    if total:
        return response_json['paging']['total']
    elif results:
        return response_json['results']
    else:
        return response_json
    
    
## LOAD COMUNAS LIST FROM MONGODB ##

# Create connection
client = MongoClient(host='localhost')
# Connect to database
db = client['mercado_libre_data']
# Find collection
col = db['comunas_santiago']
# Obtain data
comunas_list = []
for doc in col.find({}, {
    "_id": 0, 
    "name": 1, 
    "id": 1
    }):
    comunas_list.append(doc)
# Close connection    
client.close()

## EXTRACT DATA FROM API ##

items_list = []

for comuna in comunas_list:
    city_id = comuna['id']
    comuna_data = extract_data_from_api(city_id=city_id)
    comuna_total = comuna_data['paging']['total']
    print('Comuna: {} -- Total to be extracted: {}'.format(comuna['name'], comuna_total))
    
    if comuna_total > 1000:
        available_filters = comuna_data['available_filters']
        
        for i in range(len(available_filters)):
            if available_filters[i]['id']=='neighborhood':
                neighborhood_list = available_filters[i]['values']
                
                for neighborhood in neighborhood_list:
                    neighborhood_id = neighborhood['id']
                    neighborhood_data = extract_data_from_api(city_id=city_id, 
                                                              neighborhood_id=neighborhood_id)
                    neighborhood_total = neighborhood_data['paging']['total']
                    print('Neighborhood: {} -- Total to be extracted: {}'.format(neighborhood['name'], 
                                                                                 neighborhood_total))
                    if neighborhood_total > 1000:
                        available_filters = neighborhood_data['available_filters']
                        
                        for i in range(len(available_filters)):
                            if available_filters[i]['id']=='BEDROOMS':
                                n_bedrooms_list = available_filters[i]['values']
                                
                                for category in n_bedrooms_list:
                                    bd_range = category['id']
                                    bd_range_data = extract_data_from_api(city_id=city_id, 
                                                                          neighborhood_id=neighborhood_id, 
                                                                          bd_range=bd_range)
                                    bd_range_total = bd_range_data['paging']['total']
                                    print('Bedrooms: {} -- Total to be extracted: {}'.format(category['name'],
                                                                                             bd_range_total))
                                    if bd_range_total > 1000:
                                        print('Number of items to be extracted is greater than 1000!')
                                    else:
                                        offset = 0
                                        while offset < bd_range_total:
                                            results = extract_data_from_api(city_id=city_id, 
                                                                            neighborhood_id=neighborhood_id,
                                                                            bd_range=bd_range, 
                                                                            limit=LIMIT, 
                                                                            offset=offset, 
                                                                            results=True)
                                            for result in results:
                                                items_list.append(result)
                                                
                                            TIMEOUT = random.uniform(TM_MIN, TM_MAX)
                                            time.sleep(TIMEOUT)
                                            offset += LIMIT
                                            
                                        print('Number of extracted items: {}'.format(len(items_list)))
                                break   #stop looking in the available filters
                                
                        TIMEOUT = random.uniform(TM_MIN, TM_MAX)
                        time.sleep(TIMEOUT)
                        
                    else:
                        offset = 0
                        while offset < neighborhood_total:
                            results = extract_data_from_api(city_id=city_id, 
                                                            neighborhood_id=neighborhood_id, 
                                                            limit=LIMIT, 
                                                            offset=offset, 
                                                            results=True)
                            for result in results:
                                items_list.append(result)
                                
                            TIMEOUT = random.uniform(TM_MIN, TM_MAX)
                            time.sleep(TIMEOUT)
                            offset += LIMIT
                            
                        print('Number of extracted items: {}'.format(len(items_list)))
                break   #stop looking in the available filters
                
        TIMEOUT = random.uniform(TM_MIN, TM_MAX)
        time.sleep(TIMEOUT)
        
    else:
        offset = 0
        while offset < comuna_total:
            results = extract_data_from_api(city_id=city_id, 
                                            limit=LIMIT, 
                                            offset=offset, 
                                            results=True)
            for result in results:
                items_list.append(result)
                
            TIMEOUT = random.uniform(TM_MIN, TM_MAX)
            time.sleep(TIMEOUT)
            offset += LIMIT
            
        print('Number of extracted items: {}'.format(len(items_list)))

## LOAD DATA TO MONGODB ##

# Create connection
client = MongoClient(host='localhost')
# Connect to database
db = client['mercado_libre_data']
# Create collection
collection_name = '{}_{}_{}'.format(category_dict['operation'], 
                                    category_dict['property'],
                                    YEAR_MONTH)
col = db[collection_name]
print('Collection name: {}'.format(collection_name))
# Insert documents
col.insert_many(items_list)
print('Number of documents loaded into the database: ', col.count_documents({}))
# Close connection
client.close()
