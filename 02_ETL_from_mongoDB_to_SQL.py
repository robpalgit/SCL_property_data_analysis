##### IMPORT LIBRARIES #####

from pymongo import MongoClient
from sqlalchemy import create_engine

import pandas as pd
import numpy as np
import re

#from postgresql_login import postgresql_login
from utils.sql_ET import extract_avg_value_from_sql_database

##### DEFINE CONSTANTS #####

collection = 'rent_apartment_2021-06'

year = collection.split('_')[2].split('-')[0]
month = int(collection.split('_')[2].split('-')[1])

UF_VALUE = extract_avg_value_from_sql_database(month, year, usd=False)
USD_VALUE = extract_avg_value_from_sql_database(month, year, usd=True)


min_max_dict = {
    'min_bed': 1,
    'max_bed': 5,
    'min_bath': 1,
    'max_bath': 5,
    'price_perc': 0.5,
    'cov_area_perc': 0.25,
    'tot_area_perc': 0.25,
    'min_area_ratio': 0.7,
    'min_lat': -33.65,
    'max_lat': -33.25,
    'min_lon': -70.8,
    'max_lon': -70.45
}

##### DEFINE FUNCTION TO EXTRACT DATA FROM MONGODB AND CONVERT TO DATAFRAME #####

def extract_raw_data_from_mongodb(collection):
    # Create connection to MongoDB
    client = MongoClient(host='localhost')
    # Connect to database
    db = client['mercado_libre_data']
    # Find collection
    col = db[collection]

    results = col.find({}, {
        '_id': 0, 
        'id': 1,
        'price': 1, 
        'currency_id': 1, 
        'permalink': 1,
        'location': 1,
        'attributes': 1
    })

    client.close()

    id_list = []
    price_list = []
    currency_list = []
    permalink_list = []
    address_list = []
    neighborhood_list = []
    comuna_list = []
    latitude_list = []
    longitude_list = []
    bedrooms_list = []
    bathrooms_list = []
    covered_area_list = []
    total_area_list = []

    for result in results:
        # id
        property_id = result['id']
        id_list.append(property_id)
        # Price
        price = result['price']
        price_list.append(price)
        # Currency id
        currency = result['currency_id']
        currency_list.append(currency)
        # Permalink
        permalink = result['permalink']
        permalink_list.append(permalink)
        # Address line
        address = result['location']['address_line']
        address_list.append(address)
        # Neighborhood
        neighborhood = result['location']['neighborhood']['name']
        neighborhood_list.append(neighborhood)
        # Comuna
        comuna = result['location']['city']['name']
        comuna_list.append(comuna)
        # Latitude
        latitude = result['location']['latitude']
        latitude_list.append(latitude)
        # Longitude
        longitude = result['location']['longitude']
        longitude_list.append(longitude)
        # Attributes
        attributes = result['attributes']
        for i in range(len(attributes)):
            # Bedrooms
            if attributes[i]['id']=='BEDROOMS':
                bedrooms = attributes[i]['value_name']
                bedrooms_list.append(bedrooms)
            # Bathrooms
            elif attributes[i]['id']=='FULL_BATHROOMS':
                bathrooms = attributes[i]['value_name']
                bathrooms_list.append(bathrooms)
            # Covered area
            elif attributes[i]['id']=='COVERED_AREA':
                covered_area = attributes[i]['value_name'].replace('m²', '')
                covered_area_list.append(covered_area)
            # Total area
            elif attributes[i]['id']=='TOTAL_AREA':
                total_area = attributes[i]['value_name'].replace('m²', '')
                total_area_list.append(total_area)
                break
         
    ## Create dataframe
    df = pd.DataFrame(
        list(zip(id_list, price_list, currency_list, 
                 bedrooms_list, bathrooms_list, 
                 covered_area_list, total_area_list, 
                 comuna_list, neighborhood_list, address_list, 
                 latitude_list, longitude_list, 
                 permalink_list)),
        columns=['id_', 'price', 'currency', 
                 'bedrooms', 'bathrooms', 
                 'covered_area_m2', 'total_area_m2',
                 'comuna', 'neighborhood', 'address', 
                 'latitude', 'longitude', 'permalink']
    )

    df['operation'] = collection.split('_')[0]
    df['property_type'] = collection.split('_')[1]
    df['year'] = collection.split('_')[2].split('-')[0]
    df['month'] = collection.split('_')[2].split('-')[1]
    
    return df

##### DEFINE FUNCTIONS TO TRANSFORM DATA #####

def copy_data(df):
    return df.copy()

def validate_and_transform_data(df):
    for idx, row in df.iterrows():
        # Validate that all values for "bedrooms" are integer type
        if not bool(re.match('\d', df.loc[idx, 'bedrooms'])):
            df.loc[idx, 'bedrooms'] = 0
        # Validate that all values for "bathrooms" are integer type
        if not bool(re.match('\d', df.loc[idx, 'bathrooms'])):
            df.loc[idx, 'bathrooms'] = 0
        # Validate that all values for "covered_area_m2" are numeric
        if not bool(re.match('\d+\.?\d*', df.loc[idx, 'covered_area_m2'])):
            df.loc[idx, 'covered_area_m2'] = 0
        # Validate that all values for "total_area_m2" are numeric
        if not bool(re.match('\d+\.?\d*', df.loc[idx, 'total_area_m2'])):
            df.loc[idx, 'total_area_m2'] = 0
        # Validate that all values for "neighborhood" are alphabetic
        if not bool(re.match('[\w\-\s]+', df.loc[idx, 'neighborhood'])):
            df.loc[idx, 'neighborhood'] = df.loc[idx, 'comuna']
        
        # Transform all prices to CLP values (for properties on rent)
        if (df.loc[idx, 'currency']=='CLF'):
            df.loc[idx, 'price'] = round(df.loc[idx, 'price'] * UF_VALUE, 0)
            df.loc[idx, 'currency'] = 'CLP'
        elif (df.loc[idx, 'currency']=='USD'):
            df.loc[idx, 'price'] = round((df.loc[idx, 'price'] * USD_VALUE), 0)
            df.loc[idx, 'currency'] = 'CLP'

    df['bedrooms'] = df['bedrooms'].astype('int32')
    df['bathrooms'] = df['bathrooms'].astype('int32')
    df['covered_area_m2'] = df['covered_area_m2'].astype('float32')
    df['total_area_m2'] = df['total_area_m2'].astype('float32')
    df['comuna'] = df['comuna'].astype('category')
    df['neighborhood'] = df['neighborhood'].apply(lambda x: x.title()).astype('category')
    df['currency'] = df['currency'].astype('category')
    df['price'] = df['price'].astype('int32')
    df['operation'] = df['operation'].astype('category')
    df['property_type'] = df['property_type'].astype('category')
    return df

def delete_duplicate_rows(df):
    subset = ['price', 'address', 'neighborhood', 'comuna', 'latitude', 'longitude', 
              'bedrooms', 'bathrooms', 'covered_area_m2', 'total_area_m2']
    to_delete = df[df.duplicated(subset=subset)==True].index.tolist()
    df = df.drop(to_delete, axis=0)
    print('Number of removed duplicate rows:', len(to_delete))
    return df

def delete_null_values(df):
    to_delete = df[
        (df['bedrooms']==0) | 
        (df['bathrooms']==0) |
        (df['longitude'].isnull()) | 
        (df['latitude'].isnull()) |
        (df['price']==0) |
        (df['covered_area_m2']==0) |
        (df['total_area_m2']==0)
    ].index.tolist()
    df = df.drop(to_delete, axis=0)
    print('Number of deleted rows because of null values:', len(to_delete))
    return df

def delete_outliers(df,
                    min_bed, max_bed, 
                    min_bath, max_bath, 
                    price_perc, 
                    cov_area_perc, 
                    tot_area_perc, 
                    min_area_ratio, 
                    min_lat, max_lat, 
                    min_lon, max_lon):

    min_price = df['price'].quantile(price_perc / 100)
    max_price = df['price'].quantile(1 - (price_perc / 100))

    min_cov_area = df['covered_area_m2'].quantile(cov_area_perc / 100)
    max_cov_area = df['covered_area_m2'].quantile(1 - (cov_area_perc / 100))

    min_tot_area = df['total_area_m2'].quantile(tot_area_perc / 100)
    max_tot_area = df['total_area_m2'].quantile(1 - (tot_area_perc / 100))
    
    df['covered_total_area_ratio'] = df['covered_area_m2'] / df['total_area_m2']

    to_delete = df[
        (df['bedrooms'] > max_bed) |
        (df['bathrooms'] > max_bath) |
        (df['price'] < min_price) | (df['price'] > max_price) |
        (df['covered_area_m2'] < min_cov_area) | (df['covered_area_m2'] > max_cov_area) |
        (df['total_area_m2'] < min_tot_area) | (df['total_area_m2'] > max_tot_area) |
        (df['covered_area_m2'] > df['total_area_m2']) |
        (df['covered_total_area_ratio'] < min_area_ratio) |
        (df['latitude'] < min_lat) | (df['latitude'] > max_lat) |
        (df['longitude'] < min_lon) | (df['longitude'] > max_lon)
    ].index.tolist()

    df = df.drop(to_delete, axis=0).reset_index(drop=True)
    del df['covered_total_area_ratio']
    print('Number of deleted rows because of outlier values:', len(to_delete))
    return df

##### DEFINE FUNCTION TO LOAD DATAFRAME INTO A POSTGRESQL TABLE #####

def load_data_into_postgresql_table(df):
    # Prepare login info
    postgresql_str = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'\
    .format(**postgresql_login)
    # Establish connection to database
    engine = create_engine(postgresql_str)
    conn = engine.connect()
    # Load dataframe to pos
    df.to_sql('properties', conn, if_exists='append', index=False)
    print('Data loaded to database...')
    conn.close()
    
##### DEFINE MAIN FUNCTION #####
    
def main():
    # EXTRACT DATA FROM MONGODB
    raw_df = extract_raw_data_from_mongodb(collection)
    print('Number of rows of raw dataframe:', len(raw_df))
    # TRANSFORM RAW DATA
    transformed_df = (raw_df.
                      pipe(copy_data).
                      pipe(validate_and_transform_data).
                      pipe(delete_duplicate_rows).
                      pipe(delete_null_values).
                      pipe(delete_outliers, **min_max_dict))
    print('Number of rows of transformed dataframe:', len(transformed_df))
    # LOAD TRANSFORMED DATA INTO POSTGRESQL
    load_data_into_postgresql_table(transformed_df)
    
    
if __name__ == "__main__":
    main()
