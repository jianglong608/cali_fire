
# coding: utf-8

import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 
import pandas.io.sql as psql
from matplotlib import pyplot as plt
import subprocess, os
import shapely
from shapely.geometry import Point, LineString, MultiPolygon, asMultiPolygon, Polygon
from shapely import wkb, wkt
import geopandas as gpd
import math
import time
from functools import wraps


def timer(start_time):
    """
    return duration since 'start_time', will print out s, mins, hrs, days that makes sense
    """
    diff = time.time() - start_time
    if diff <= 60:
        duration = '{} s'.format(round(diff, 2))
    if diff > 60 and diff <= 3600:
        duration = '{} mins'.format(round(diff/60, 2))
    if diff > 3600:
        duration = '{} hrs'.format(round(diff/3600, 2))
    if diff > 24*60*60:
        duration = '{} days'.format(round(diff/(3600*24), 2))
    return duration


def function_timer(func):
    @wraps(func)
    def time_a_func(*args, **kwargs):
        t0 = time.time()
        result = func(*args, **kwargs)
        duration = timer(t0)
        print("Total time running %s is: %s" % (func.__name__, timer(t0)))
        return result
    return time_a_func



def gisConnect(dbname):
    """
    create connection to postgis database 'dbname', return connection and cursor
    """
#     dbname = 'tampa'
    try:
        connection = psycopg2.connect(host='localhost', database=dbname, user='postgres', password='byesbhamp')
    except:
        print("I am unable to connect to the database")
    cursor = connection.cursor()
    return connection, cursor



def import_hve_csv(csv_file):
    """
    - csv_file: path + filename of hve csv file
    """
#    csv_file = "Z:/he22p2v9/Creditmod/Jianglong/{file}".format(file= csv_file)
    df = pd.read_csv(csv_file, dtype={'address': str,
                                      'zip': str,
                                      'city': str,
                                      'lat': float,
                                      'long': float})
    # df['long'] = -df['long'] # hve data longitude doesn't come with sign
    df['long'] = df['long'].apply(lambda x: -x if x > 0 else x)  # hve data longitude doesn't come with sign
    return df


def drop_postgis_table(dbname, tbname, keyword=False):
    """
    drop table 'tbname' from database 'dbname', if keyword == True, drop all tables with 'tbname' in it's name
    ------------------------------
    """
    engine = create_engine(r'postgresql://postgres:byesbhamp@localhost:5432/%s' % dbname)
    if keyword: 
        tbl_drop = [tbl for tbl in engine.table_names() if tbname.lower() in tbl.lower()]
    else:
        tbl_drop = [tbl for tbl in engine.table_names() if tbname.lower() == tbl.lower()]
    for tbl in tbl_drop:
        engine.execute('drop table if exists {hve}'.format(hve=tbl))
    print("tables {tables} dropped from databse {dbname}".format(tables = tbl_drop, dbname=dbname))


def transform_pd_to_gpd_general(df,  crs={'init' :'epsg:4326'}, geometry='geom_schzone_high', gtype='wkt'):
    """
    take a pandas df including wkt geometry columns and transform to geopandas gdf
    ----------------
    df: pandas df
    crs: {'init' :'epsg:4326'} eg.
    geometry: the geom column you want to set for gdf, if doesn't exist will return with None geometry col
    gtype: wkt or wkb, from which format convert to geomery
    """
    data = df.copy()

    # transform all wkt geometry type to shapely geometry data type
    if gtype == 'wkt':
        data[geometry] = data[geometry].apply(lambda x: wkt.loads(x) if type(x) == str else None)
    if gtype == 'wkb':
        data[geometry] = data[geometry].apply(lambda x: wkb.loads(x, hex=True) if type(x) == str else None)
            
#     transform df_GIS into a geopandas DataFrame
    gdf = gpd.GeoDataFrame(data, crs = crs, geometry=geometry) 
    return gdf



def transform_pd_to_gpd(df,  crs={'init' :'epsg:4326'}, geometry='geom_schzone_high', gtype='wkt'):
    """
    take a pandas df including wkt geometry columns and transform to geopandas gdf
    ----------------
    df: pandas df
    crs: {'init' :'epsg:4326'} eg.
    geometry: the geom column you want to set for gdf, if doesn't exist will return with None geometry col
    gtype: wkt or wkb, from which format convert to geomery
    """
    data = df.copy()
    # get all the geometry columns
    geom_columns = [col for col in list(data.columns) if 'geom_' in col]

    # transform all wkt geometry type to shapely geometry data type
    for col in geom_columns:
        if gtype == 'wkt':
            data[col] = data[col].apply(lambda x: wkt.loads(x) if type(x) == str else None)
        if gtype == 'wkb':
            data[col] = data[col].apply(lambda x: wkb.loads(x, hex=True) if type(x) == str else None)
            
    # transform df_GIS into a geopandas DataFrame
    geom = None if geometry not in geom_columns else geometry
    gdf = gpd.GeoDataFrame(data, crs = crs, geometry=geom) 
    return gdf



def convert_wkt_to_wkb(df):
    """
    take a pandas df including wkt geometry columns and transform to wkb format
    ----------------
    df: pandas df
    """
    
    data = df.copy()
    geom_columns = [col for col in list(df.columns) if 'geom_' in col]

    # transform all wkt format to wkb format
    for col in geom_columns:
        data[col] = data[col].apply(lambda x: wkb.dumps(wkt.loads(x), hex=True) if type(x) == str else None)
    return data



def upload_dataframe_postgis(df, dbname, tbname):
    """
    upload pandas dataframe 'df' into postgis databse 'dbname' and create table 'tbname'
    """
#     dbname = 'tampa'
    data = df.rename(columns=lambda col: col.lower()) # rename to make sure sql take the col names
    engine = create_engine(r'postgresql://postgres:byesbhamp@localhost:5432/%s' % dbname)
    data.to_sql(tbname, engine, if_exists='replace', index=False)

def upload_csv_postgis(csv, dbname, tbname):
    """
    - csv: name of the csv file to upload to postgis
    - dbname: database name
    - tbname: name of the table to be created
    """
    df = import_hve_csv(csv_file=csv)
    upload_dataframe_postgis(df=df, dbname=dbname, tbname=tbname)
    print('%s file successfully uploaded to database %s as table %s' % (csv, dbname, tbname))

def upload_geodataframe_postgis(gdf, dbname, tbname):
    """
    upload geopandas dataframe 'gdf' into postgis databse 'dbname' and create table 'tbname'
    """
    gd = gdf.copy()
    col_geom = gd.geometry.name
    cols_other = list(set(list(gd.columns)) - set([col_geom]))
    sql_select = ','.join(cols_other) + ',ST_SetSRID(ST_GeomFromText({col_geom}), 5070) as {col_geom}'
    
    gd[col_geom] = gd.geometry.apply(lambda x: wkt.dumps(x)) # convert geometry column to wkt for loading
    upload_dataframe_postgis(df=gd, dbname=dbname, tbname='temp')

    # initate connection with postgis and execute sql to create table sabs
    connection, cursor = gisConnect(dbname=dbname)
    cursor.execute("""
drop table if exists {tbname};
create table {tbname} as 
select {sql_select}
from temp;
drop table if exists temp;
create index geom_index_{tbname}
on {tbname} using gist({col_geom});""".format(tbname=tbname, 
                                              sql_select=sql_select.format(col_geom=col_geom),
                                              col_geom=col_geom))
    connection.commit()
    connection.close()
    print("geodataframe successfully uploaded to database {dbname} as table {tbname}".format(dbname=dbname, 
                                                                                             tbname=tbname))

def upload_shapefile_postgis(pathfile, dbname, tbname):
    """
    upload shapefile 'pathfile' into postgis databse 'dbname' and create table 'tbname', create index on geom col
    """
    gdf = gpd.read_file(pathfile)
    upload_geodataframe_postgis(gdf=gdf, dbname=dbname, tbname=tbname)



def create_postgisDB(dbname):
    """
    create postgis database
    """
#     dbname = "tampa"
    connection = psycopg2.connect(host='localhost', database='postgres', user='postgres', password='byesbhamp')
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
    cur = connection.cursor()
    cur.execute("CREATE DATABASE %s;" % dbname)
    connection.close()

    connection2 = psycopg2.connect(host='localhost', database=dbname, user='postgres', password='byesbhamp')
    connection2.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur2 = connection2.cursor()
    cur2.execute("CREATE EXTENSION postgis;")
    connection2.close()
    print("PoastGIS database %s successfully created!" % dbname)



def convert_geomTo4326_byPostgis(gdf, dbname, tbname='gdf_temp', column='geometry'):
    upload_geodataframe_postgis(gdf, dbname, tbname)
    connection, cursor = gisConnect(dbname=dbname)
    query = """
select *
      ,ST_asText(ST_Transform({geometry}, 4326)) as geometry_4326
from {tbname}
""".format(geometry=column, tbname=tbname)
    result = psql.read_sql_query(query, connection)
    result.drop(column, axis=1, inplace=True)
    result.rename(columns={'geometry_4326': 'geometry'}, inplace=True)
    result = transform_pd_to_gpd_general(result, geometry='geometry')
    drop_postgis_table(dbname, tbname)
    return result



def readin_df_GIS(path, file):
    """
    read in df_GIS csv result from path/file, the data schemas are not programatically robust yet, further work needed.
    """
    df = pd.read_csv('{path}/{file}'.format(path=path, file=file), 
                     dtype={'zip_house': str
                            ,'ncessch_middle': str
                            ,'ncessch_primary': str
                            ,'ncessch_high': str
                            ,'name_coastline': str
                            ,'osm_id_hospital': str
                            ,'osm_id_landfill': str
                            ,'osm_id_prison': str
                            ,'osm_id_cemetery': str
                            ,'osm_id_airrunway': str
                            ,'osm_id_highway': str
                            ,'osm_id_railway': str
                            ,'osm_id_coastline': str
                            ,'osm_id_bay': str
                            ,'osm_id_beach': str
                            ,'osm_id_water': str
                            ,'osm_id_retail': str
                            ,'osm_id_shop': str
                            ,'osm_id_power_line': str
                            ,'osm_id_river': str
                            ,'name_landfill': str
                            ,'name_power_line': str})
    df_gis = transform_pd_to_gpd(df, crs={'init' :'epsg:4326'}, geometry='geom_schzone_high')
    return df_gis



def get_boundary(df, quantile=1, crs=4326):
    low = (1-quantile)/2; high = quantile + low
    lat = 'lat'; lng = 'long'
    # set up four corners
    lower = df[lat].quantile(low)
    upper = df[lat].quantile(high)
    left = df[lng].quantile(low)
    right = df[lng].quantile(high)
    boundbox = (left, lower, right, upper, crs)
    return boundbox



def getInBoxPolygons(gdf, df=None, left=None, lower=None, right=None, upper=None):
#     gdf = fl
    if df is not None:
        left, lower, right, upper, crs = get_boundary(df)
    boundary = Polygon([(left, lower), (left, upper), (right, upper), (right, lower)])
    dfbox = gpd.GeoDataFrame({'geometry': [boundary], 'desc': ['boundary']}, crs={'init' :'epsg:4326'}, geometry='geometry')
    gdf_inBox = gdf[gdf.apply(lambda row: boundary.intersects(row['geometry']), axis=1)]
    return gdf_inBox



def getInAreaSchools(df, dbname, quantile=0.9999, crs=4326, printonly=False):
    text = """
select *
      ,st_asText(st_transform(geometry, 4326)) as geometry_new
from sabs
where st_intersects(st_transform(geometry, {crs}), st_MakeEnvelope{boundary});
"""
    if not printonly:
        connection, cursor = gisConnect(dbname=dbname)
    boundary = get_boundary(df, quantile=quantile, crs=crs)
    query = text.format(crs = crs, boundary = boundary)
    if printonly:
        print(query)
    else:
        result = psql.read_sql_query(query, connection)
        result.drop('geometry', axis=1, inplace=True)
        result.rename(columns={'geometry_new': 'geometry'}, inplace=True)
        result['geometry'] = result['geometry'].apply(lambda x: wkt.loads(x))      
        gdata = gpd.GeoDataFrame(result, crs={'init' :'epsg:4326'}, geometry='geometry')  
        connection.close()
        return gdata
