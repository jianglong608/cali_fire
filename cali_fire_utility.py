
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb
import numpy as np
from shapely.geometry import Point, LineString, MultiPolygon, asMultiPolygon, Polygon
from shapely import wkb, wkt
import shapely
import geopandas as gpd
from shapely.ops import unary_union
import requests
from bs4 import BeautifulSoup
import re
import os
import zipfile
import wget
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool 

import sys
from mapping_utility_v2 import map_geopandas, map_AllHouses
from mapping_utility_fire import map_geopandas_fire
from python_postgis_talk_utility import transform_pd_to_gpd_general, transform_pd_to_gpd

##########################
## geoprocessing functions
##########################

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
    if df is not None:
        left, lower, right, upper, crs = get_boundary(df)
    boundary = Polygon([(left, lower), (left, upper), (right, upper), (right, lower)])
    dfbox = gpd.GeoDataFrame({'geometry': [boundary], 'desc': ['boundary']}, crs={'init' :'epsg:4326'}, 
                             geometry='geometry')
    gdf_inBox = gdf[gdf.apply(lambda row: boundary.intersects(row['geometry']), axis=1)]
    return gdf_inBox

def geomatch(data, polygon, prmrkey_plgon, cols=None):
    cols_plgon = polygon.columns if cols is None else cols
    df = data.copy()
    gdf = getInBoxPolygons(polygon, df)
    df['geometry'] = df.apply(lambda row: Point(row['long'], row['lat']), axis=1)
    df = gpd.GeoDataFrame(df, crs={'init' :'epsg:4326'})
    result = gpd.sjoin(left_df=df, right_df=gdf[cols_plgon], how='left', op='intersects')
    result.drop(['geometry', 'index_right'], axis=1, inplace=True)
    result = result.merge(right=gdf[[prmrkey_plgon, 'geometry']], how='left', on = prmrkey_plgon)
    return result

def readin_shapefile(path_file):
    gdf = gpd.read_file(path_file)
    gdf = gdf.to_crs(crs={'init' :'epsg:4326'})
    gdf = gdf.rename(columns=lambda x: x.lower())
    return gdf

def timer():
    now = datetime.now()
    tformat = "%H:%M:%S"
    time = now.strftime(tformat)
    return time

#############################
# fire data scraping function
#############################

def getsoup(url):
    res = requests.get(url)
#     res.raise_for_status()
    if res.ok == True:   # make sure the link isvalid and will not return error like 404
        soup = BeautifulSoup(res.content, "lxml")
        return soup
    else:
        pass

def get_firelinks(url, url_master):
    soup = getsoup(url)
    links = {}
    for inc in soup('a'):
        link = inc.attrs['href']
        name = inc.text
        if name != '[To Parent Directory]':
            links[name] = url_master + link
    return links

def get_zipfiles(url, url_master, download_type='zip'):
    soup = getsoup(url)
    links = []
    for inc in soup('a'):
        link = inc.attrs['href']
        name = inc.text
        if name != '[To Parent Directory]':
            links.append(url_master + link)
    if download_type == 'zip':
        links = [link for link in links if '.zip' in link]
    elif download_type == 'shp':
        links = [link for link in links if '.zip' not in link]
    return links

def get_firezips(url, url_master, download_type='zip'):
    fire_links = get_firelinks(url, url_master)
    size = 0
    for fire in fire_links:
        zips = get_zipfiles(fire_links[fire], url_master, download_type)
        size += len(zips)
        fire_links[fire] = zips
    fire_zips = fire_links
    print('totally %s zip files url parsed! %s' % (size, timer()))
    return fire_zips

def mkfolder(root, folder_list, verb=True):
    for sub in folder_list:
        if sub not in os.listdir(root):
            os.mkdir('%s/%s' % (root, sub))
            if verb: print('++++++++ folder %s created!' % (root + '/' + sub))

def download_fires(url, url_master, folder_root, folder_sub='cali_fire', download_type='zip', unzip=False, verb=True):
    # get all the fire shapefile link
    fire_zips = get_firezips(url, url_master, download_type)
    
    # create cali_fire from root folder
    mkfolder(folder_root, [folder_sub], verb)
    folder_cali = folder_root + '/' + folder_sub
    
    # for each fire incident create a sub folder and download the revevant files
    for fire in fire_zips:
        mkfolder(folder_cali, [fire]) # 1.create sub folder
        for url in fire_zips[fire]:
            pathfile = '/'.join([folder_root, folder_sub, fire, url.split('/')[-1]])
            if not os.path.exists(pathfile):
                path = folder_cali + '/' + fire
                file = wget.download(url, out=path) # 2.download (zip) files
                if verb: print('file %s downloaded!' % file)
                if unzip:
                    with zipfile.ZipFile(file, "r") as zip_ref: 
                        zip_ref.extractall(path=path) # 3.unzip file (optional)

def unzip_check(folder_root, folder_sub, verb=True):
    folder_cali = folder_root + '/' + folder_sub
    for folder in os.listdir(folder_cali):
        if folder != '.DS_Store':
            all_files = os.listdir(folder_cali + '/' + folder)
            zfiles = [file for file in all_files if file.split('.')[-1] == 'zip']
            for zfile in zfiles:
                shpfile = zfile.replace('zip', 'shp')
                pathshpfile = '/'.join([folder_cali, folder, shpfile])
                pathzfile = '/'.join([folder_cali, folder, zfile])
                if not os.path.exists(pathshpfile):
                    with zipfile.ZipFile(pathzfile, "r") as zip_ref: 
                        zip_ref.extractall(path=folder_cali + '/' + folder) # 3.unzip file (optional)
                    if verb: print('zip file %s got unzipped' % pathzfile)                        
                        
def retrieve_shp(folder_root, folder_sub):
    all_gdf = []
    for folder in os.listdir(folder_root + '/' + folder_sub):
        if re.match(r'\.\w+', folder) is None:
            for file in os.listdir('/'.join([folder_root, folder_sub, folder])):
                if file.split('.')[-1] == 'shp':
                    gdf = readin_shapefile('/'.join([folder_root, folder_sub, folder, file]))
                    all_gdf.append(gdf)
    df = pd.concat(all_gdf, ignore_index=True)
    return df


def download_and_create_shp(url_cali, url_master, folder_root, folder_sub, download_type='zip', unzip=True, verb=True):
    if verb: print("\n########## scraping start: %s ##########" % timer())
    download_fires(url_cali, url_master, folder_root, folder_sub, download_type, unzip, verb)
    
    if verb: print("\n########## check unzipped files: %s ##########" % timer())
    unzip_check(folder_root, folder_sub, verb)
    
    df = retrieve_shp(folder_root, folder_sub)
    return df

def download_read_curent_fire(folder_root, folder_sub = 'active_fire_US'):
    path = '/'.join([folder_root, folder_sub])
    mkfolder(folder_root, [folder_sub])
    if os.path.exists('/'.join([folder_root, folder_sub, 'active_perimeters_dd83.zip'])):
        os.remove('/'.join([folder_root, folder_sub, 'active_perimeters_dd83.zip']))
    url_active = """https://rmgsc.cr.usgs.gov/outgoing/GeoMAC/current_year_fire_data/current_year_all_states/active_perimeters_dd83.zip"""    
    file = wget.download(url=url_active, out=path)
    with zipfile.ZipFile(file, "r") as zip_ref: 
        zip_ref.extractall(path=path) # 3.unzip file (optional)
    pathfile = '/'.join([folder_root, folder_sub, 'active_perimeters_dd83.shp'])
    gdf = readin_shapefile(pathfile)
    return gdf



####################################
# fire data post processing function
####################################



def write_union_totxt(gdf, group, grp):
    grouped = gdf.loc[grp.groups[group],].groupby('firename')
    geometry = grouped['geometry2'].agg({'geometry': unary_union})
    acres = grouped['gisacres'].agg({'max_gisacres': np.max})
    date = grouped['perdattime'].agg({'start_date': np.min, 'end_date': np.max})
    union = pd.concat([geometry, acres, date], axis=1).reset_index()
    return union

def fire_postprocessing(gdf):
    # name normalize
    gdf.loc[:, 'firename'] = gdf.firename.str.upper()
    gdf.loc[:, 'firename'] = gdf.firename.str.strip()

    # structure geometry2 and perdattime
    gdf.loc[:,'geometry2'] = gdf.apply(lambda row: row['geometry'].buffer(0) if not row['geometry'].is_valid 
                                       else row['geometry'], axis=1)
    gdf.loc[:, 'perdattime'] = pd.to_datetime(gdf.perdattime)
    return gdf

def create_fire_union(gdf, workers=6):
    grp = gdf.groupby('firename')
    all_groups = list(grp.groups.keys())

    pool = ThreadPool(workers)
    all_union = pool.map(lambda x: write_union_totxt(gdf, x, grp), all_groups)
    gdf_union = pd.concat(all_union, ignore_index=True)

    gdf_union = transform_pd_to_gpd(gdf_union, geometry='geometry')
    
    return gdf_union

