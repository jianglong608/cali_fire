
# coding: utf-8

import psycopg2
import pandas as pd
import numpy as np
import sqlalchemy
import pandas.io.sql as psql
from matplotlib import pyplot as plt
import folium  
# import mplleaflet
import shapely
import math

from shapely.geometry import Point
from shapely import wkb, wkt
from folium import plugins
from folium import IFrame
from IPython.display import IFrame, display
import seaborn as sns 
import branca

import json
import geopandas as gpd
from python_postgis_talk_utility import transform_pd_to_gpd


def rename_for_mapping(df):
    columns={'zip': 'zip_house', 'address': 'address_house', 'lat': 'lat_house', 
             'long': 'lng_house', 'city': 'city_house', 'gsrating_primary': 'gsRating_primary',
             'gsrating_middle': 'gsRating_middle', 'gsrating_high': 'gsRating_high'}
    df = df.rename(columns=columns)
    return df



# find all the school columns
cols_school = ['schoolname_middle', 'ncessch_middle', 'geom_schzone_middle', 'lat_school_middle', 'lng_school_middle',
               'distance_school_middle', 'gsrating_middle', 'schoolname_primary', 'ncessch_primary', 
               'geom_schzone_primary', 'lat_school_primary', 'lng_school_primary', 'distance_school_primary', 
               'gsrating_primary', 'schoolname_high', 'ncessch_high', 'geom_schzone_high', 'lat_school_high', 
               'lng_school_high', 'distance_school_high', 'gsrating_high']



# set up popup text
html_home = u"""
<!DOCTYPE html>
<html>
<head>
<style>
table, th, td {{
    border: 1px solid black;
    border-collapse: collapse;
/}}
</style>
</head>
<body>

<table style="id=home">
  <tr>
    <td>zip code</td>
    <td>{zip}</td> 
  </tr>
  <tr>
    <td>address</td>
    <td>{address}</td>
  </tr>
  <tr>
    <td>city</td>
    <td>{city}</td>
  </tr>
  <tr>
    <td>latitude</td>
    <td>{lat}</td>
  </tr>
  <tr>
    <td>longitude</td>
    <td>{long}</td>
  </tr>
</table>

</body>
</html>""".format



html_home_distance = u"""
<!DOCTYPE html>
<html>
<head>
<style>
table, th, td {{
    border: 1px solid black;
    border-collapse: collapse;
/}}
</style>
</head>
<body>

<table style="id=home">
  <tr>
    <td>zip code</td>
    <td>{zip}</td> 
  </tr>
  <tr>
    <td>address</td>
    <td>{address}</td>
  </tr>
  <tr>
    <td>city</td>
    <td>{city}</td>
  </tr>
  <tr>
    <td>latitude</td>
    <td>{lat}</td>
  </tr>
  <tr>
    <td>longitude</td>
    <td>{long}</td>
  </tr>
  <tr>
    <td>distance_{amenity}</td>
    <td>{distance}</td>
  </tr>
</table>

</body>
</html>""".format



html_GIS = u"""
<!DOCTYPE html>
<html>
<head>
<style>
table, th, td {{
    border: 1px solid black;
    border-collapse: collapse;
/}}
</style>
</head>
<body>

<table style="id=GIS">
  <tr>
    <td>type</td>
    <td>{type}</td>
  </tr>
  <tr>
    <td>osm id</td>
    <td>{id}</td> 
  </tr>
  <tr>
    <td>name</td>
    <td>{name}</td>
  </tr>
  <tr>
    <td>distance</td>
    <td>{distance}</td>
  </tr>
</table>

</body>
</html>
""".format



html_school = u"""
<!DOCTYPE html>
<html>
<head>
<style>
table, th, td {{
    border: 1px solid black;
    border-collapse: collapse;
/}}
</style>
</head>
<body>

<table style="id=school">
  <tr>
    <td>school type</td>
    <td>{type}</td>
  </tr>
  <tr>
    <td>school name</td>
    <td>{schoolname}</td> 
  </tr>
  <tr>
    <td>nces ID</td>
    <td>{ncessch}</td>
  </tr>
  <tr>
    <td>latitude</td>
    <td>{lat_school}</td>
  </tr>
  <tr>
    <td>longitude</td>
    <td>{lng_school}</td>
  </tr>
  <tr>
    <td>distance</td>
    <td>{distance_school}</td>
  </tr>
  <tr>
    <td>gsRating</td>
    <td>{gsrating}</td>
  </tr>
</table>

</body>
</html>
""".format


# def get_boundbox(df):
#     boundbox = [[df.lat.min(), df.long.min()], [df.lat.max(), df.long.min()], 
#     [df.lat.max(), df.long.max()], [df.lat.min(), df.long.max()], [df.lat.min(), df.long.min()]]   
#     return boundbox


def get_boundbox(df, quantile=1):
    low = (1-quantile)/2; high = quantile + low
    lat = 'lat'; lng = 'long'
    # set up four corners
    lower_left  = [df[lat].quantile(low),  df[lng].quantile(low)]
    upper_left  = [df[lat].quantile(high), df[lng].quantile(low)]
    upper_right = [df[lat].quantile(high), df[lng].quantile(high)]
    lower_right = [df[lat].quantile(low),  df[lng].quantile(high)]
    # return bounding box
    boundbox = [lower_left, upper_left, upper_right, lower_right, lower_left]
    return boundbox



def map_geopandas_fire(gdf, ckeep, clabel, cpop, zoom_start=8, saveTo=None, saveName=None, saveOnly=False, houses=None):
    """
    mapping the polygon in a geopandas file, label the polygon using column clabel.
    
    """
    # modify gdf for geojson conversion
    gdf = gdf[ckeep]
    gdf = gdf.assign(style = [{'fillColor': '#e2c541', 'weight': .5, 'color': 'black'}] * gdf.shape[0])
    # centroid = gdf.geometry.unary_union.centroid
    # center_map = centroid.y, centroid.x   # has a bug when polygon has a hole
    center_map = gdf.centroid.apply(lambda coord: coord.y).median(), gdf.centroid.apply(lambda coord: coord.x).median()
    
    # set up map center and style
    mapa = folium.Map(location=center_map, tiles=None, zoom_start=zoom_start)
    lyr = folium.FeatureGroup(clabel)
    lyr_houses = folium.FeatureGroup('houses')

    
    # add each geojson row and label in the popup
    for i in range(gdf.shape[0]):
        data = gdf.iloc[i:i+1]
        geojson = folium.GeoJson(data, smooth_factor=0.01)
        geojson.add_child(folium.Popup(data[cpop].iloc[0].to_string()))
        geojson.add_to(lyr)
    lyr.add_to(mapa)

    if houses is not None:
        cols_house = ['zip', 'address', 'city', 'lat', 'long', 'color']
        houses = houses[cols_house]
        radius = 0.5
        # highlight houses
        for i in range(houses.shape[0]):
            house = houses.iloc[i]
            param = dict(house)
            # iframe = IFrame(html_home2(**param), width=250, height=150)
            iframe = branca.element.IFrame(html=html_home(**param), width=250, height=150)
            pop_home = folium.Popup(iframe, max_width=2650) 
            folium.CircleMarker(location=(float(house['lat']), float(house['long'])), 
                                radius=radius, color=param['color'], fill_color=param['color'], 
                                popup=pop_home
        #                         popup='{lat},{long}'.format(lat=houses.loc[i, 'lat'], long=houses.loc[i,'long'])
                               ).add_to(lyr_houses)
        lyr_houses.add_to(mapa)

    # adding layers to map and set up toggler and control

    folium.TileLayer('OpenStreetMap').add_to(mapa)
    folium.TileLayer('CartoDBpositron').add_to(mapa)
    folium.plugins.ScrollZoomToggler().add_to(mapa)
    folium.LayerControl().add_to(mapa)
    folium.LatLngPopup().add_to(mapa)

    if saveTo is None:
        return mapa
    else:
        mapa.save("{path}/{file}.html".format(path=saveTo, file=saveName))
        return IFrame("{path}/{file}.html".format(path=saveTo, file=saveName), width=1000, height=500) if not saveOnly else None






def map_AllHouses(df_GIS, limit=100, radius = 0.5, zoom_start = 11, saveTo = None, 
    saveName = None, saveOnly=False, box=False, bquantile=0.9999, schools=None):
    """
    plot all houses on a osm map, with each denoted as a color dot, with 'radius' as the radius of dots
    """
    # cols_house = ['zip_house', 'address_house', 'city_house', 'lat_house', 'lng_house']
    cols_house = ['zip', 'address', 'city', 'lat', 'long']
    houses = df_GIS[cols_house].sample(limit)
#     radius = 0.5
#     zoom_start = 11

    # set map center
    center_map = list(df_GIS[['lat', 'long']].mean())

    # set up popup icons
    icon_info = folium.Icon(icon='info-sign')
    icon_home = folium.Icon(icon='home')
    icon_school = folium.Icon(icon='graduation-cap', prefix='fa', color='green')
    icon_trash = folium.Icon(icon='trash', color='black')
    icon_hospital = folium.Icon(icon='plus-square', prefix='fa', color='red')

    # set up base map
    mapc = folium.Map(location=center_map, tiles=None, zoom_start=zoom_start)

#     # set up layers
#     lyr1 = folium.FeatureGroup('school')
    lyr_houses = folium.FeatureGroup('houses')
    lyr_boundbox = folium.FeatureGroup('boundbox')
    lyr_school = folium.FeatureGroup('school')
    lyr_schoolzone = folium.FeatureGroup('schoolzone')
#     lyr3 = folium.FeatureGroup('school boundary')


    # highlight houses
    for i in range(houses.shape[0]):
        house = houses.iloc[i]
        param = dict(house)
        # iframe = IFrame(html_home2(**param), width=250, height=150)
        iframe = branca.element.IFrame(html=html_home(**param), width=250, height=150)
        pop_home = folium.Popup(iframe, max_width=2650) 
        folium.CircleMarker(location=(float(house['lat']), float(house['long'])), 
                            radius=radius, color='red', fill_color='red', 
                            popup=pop_home
    #                         popup='{lat},{long}'.format(lat=houses.loc[i, 'lat'], long=houses.loc[i,'long'])
                           ).add_to(lyr_houses)
    lyr_houses.add_to(mapc)

    # render bounding box
    if box:
        boundbox1 = get_boundbox(df_GIS, quantile=1)
        boundbox2 = get_boundbox(df_GIS, quantile=bquantile)
        folium.PolyLine(boundbox1, color='red', weight=1, popup='bounding box').add_to(lyr_boundbox)
        folium.PolyLine(boundbox2, color='orange', weight=1, popup='quantile %s box' % bquantile).add_to(lyr_boundbox)
        lyr_boundbox.add_to(mapc)

    # render school zones
    if schools is not None:
        schlayers = createSchoolLayers(schools)
        for lyr in schlayers:
            lyr.add_to(lyr_schoolzone)
            # lyr.add_to(mapc)
        lyr_schoolzone.add_to(mapc)

    # render school location
    if schools is not None:
        schools_nn = schools[(-schools.lat.isnull()) & (-schools.lon.isnull())]
        for i in range(schools_nn.shape[0]):
            school = schools_nn.iloc[i]
            pop_school = folium.Popup("""
name: {schnam}
id: {ncesid}
openenroll: {openenroll}
rating: {gsrating}
level: {slevel}
gslo: {gslo}
gshi: {gshi}
""".format(**dict(school[['schnam', 'ncesid', 'openenroll', 'gsrating', 'slevel', 'gslo', 'gshi']])))
            folium.CircleMarker(location=(float(school['lat']), float(school['lon'])),
                                radius=0.8, color='green', fill_color='green', 
                                popup=pop_school).add_to(lyr_school)
        lyr_school.add_to(mapc)


    # render the layers
    folium.TileLayer('OpenStreetMap').add_to(mapc)
    folium.TileLayer('stamentoner').add_to(mapc)
    folium.TileLayer('Stamen Terrain').add_to(mapc)
    folium.TileLayer('CartoDBpositron').add_to(mapc)

    # adding map control toggler
    # folium.LatLngPopup().add_to(mapc)
    folium.plugins.ScrollZoomToggler().add_to(mapc)
    folium.LayerControl().add_to(mapc)


    if saveTo == None:
        return mapc
    else:
        mapc.save("{path}/{file}.html".format(path=saveTo, file=saveName))
        return IFrame("{path}/{file}.html".format(path=saveTo, file=saveName), width=1000, height=500) if not saveOnly else None



