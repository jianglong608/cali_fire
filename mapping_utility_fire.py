
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


# ### loading df_GIS data

# def transform_pd_to_gpd(df, geometry, crs={'init' :'epsg:4326'}):
#     """
#     take a pandas df including wkt geometry columns and transform to geopandas gdf
#     ----------------
#     df: pandas df
#     crs: {'init' :'epsg:4326'} eg.
#     geometry: the geom column you want to set for gdf
#     """
# #     geometry='geom_schzone_high'
# #     crs = {'init' :'epsg:4326'}
#     # get all the geometry columns
#     dfcp = df.copy() # to not modify original dataframe
#     geom_columns = [col for col in list(dfcp.columns) if 'geom_' in col]

#     # transform all wkt geometry type to shapely geometry data type
#     for col in geom_columns:
# #         df[col] = df[col].apply(lambda x: wkt.loads(x) if type(x) == str else None)
#         dfcp[col] = dfcp[col].apply(lambda x: wkt.loads(x) if type(x) != float else None) # math.nan is a float type

#     # transform df_GIS into a geopandas DataFrame
#     gdf = gpd.GeoDataFrame(dfcp, crs = crs, geometry=geometry)
#     return gdf


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




def createSchoolLayers(gdf, clabel='schnam', 
                     cpopup=['schnam', 'ncesid', 'openenroll', 'gsrating', 'slevel', 'gslo', 'gshi'],
                     ckeep=None):
    """
    create school layers for folium mapping
    """
    # modify gdf for geojson conversion
    if ckeep is not None:
        gdf = gdf[ckeep]
    gdf = gdf.assign(style = [{'fillColor': '#e2c541', 'weight': .5, 'color': 'black'}] * gdf.shape[0])
    
    # create layer list 
    lyrlist = []
    for i in range(gdf.shape[0]):
        data = gdf.iloc[i:i+1]
        lyr = folium.FeatureGroup(data[clabel].iloc[0])
        geojson = folium.GeoJson(data, smooth_factor=0.01)
        geojson.add_child(folium.Popup("""
name: {schnam}
id: {ncesid}
openenroll: {openenroll}
rating: {gsrating}
level: {slevel}
gslo: {gslo}
gshi: {gshi}
""".format(**dict(data[cpopup].iloc[0]))))
        geojson.add_to(lyr)
        lyrlist.append(lyr)
        lyrlist.sort(key=lambda x: x.layer_name)
    return lyrlist






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




def key_trans(key):
    if 'distance' in key:
        return 'distance'
    elif 'name' in key:
        return 'name'
    elif 'id' in key:
        return 'id'

def school_trans(key, ext):
    return key.replace('_' + ext, '')
    
    
# create a dictionary store geometry features, centroid and popup information for each amenities
def get_GISFeaturesForHouse(df_GIS, n=0, zipcode=None, address=None, city=None):
    """
    create a dictionary store geometry features, centroid and popup information for a single house.
    will spit out 'house' as a pd.Series and geodict as a dictionary.
    ---------------------
    n: the nth row/house in df_GIS
    
    """
    geodict = {}
    geom_columns = [col for col in list(df_GIS.columns) if 'geom_' in col] # get all geometry columns
    # cols_house = ['zip_house', 'address_house', 'city_house', 'lat_house', 'lng_house']
    cols_house = ['zip', 'address', 'city', 'lat', 'long']

    # select row based on parameters
    if zipcode is None: 
        house = df_GIS[cols_house].iloc[n]
    else:
        cond1 = df_GIS['zip'] == zipcode
        cond2 = df_GIS['address'] == address
        cond3 = df_GIS['city'] == city
        condition = cond1 & cond2 & cond3
        house = df_GIS[condition][cols_house].iloc[0]
    
    for geom in geom_columns:
        # take school zone out since they do not follow the same pattern
        if 'schzone' not in geom:
            amenity = '_'.join(geom.split('_')[1:])
            base_geojson = ['osm_id_', 'name_', 'distance_', 'geom_']  
            cols_popup = [i+amenity for i in list(set(base_geojson) - set(['geom_']))] # define popoup columns
            cols_geojson = [base + amenity for base in base_geojson] # get columns to include in geojson data

            # get geodataframe for each feature and it's centroid, ready for plot
            if zipcode is None:
                dframe = df_GIS[cols_geojson].iloc[n: n+1]
            else:
                dframe = df_GIS[cols_geojson][condition]
            gdframe = gpd.GeoDataFrame(dframe, crs = {'init': 'epsg:4326'}, geometry = 'geom_' + amenity)
            gdframe['style'] = [{'fillColor': '#ff0000', 'weight': .5, 'color': 'black'}]
            centroid = gdframe.centroid.iloc[0]
            popdict = dict(gdframe[cols_popup].iloc[0])
            geodict[amenity] = {'gdf': gdframe
                               ,'centroid': centroid
                               ,'popup': {key_trans(key) : popdict[key] for key in popdict}
                               }
        else:
            school = geom.split('_')[-1]
            cols_geojson = [col for col in cols_school if school in col]  #create columns goes into the geojson file
            cols_popup = list(set(cols_geojson) - set(['geom_schzone_' + school]))

            # get geodataframe for each feature and it's centroid, ready for plot
            if zipcode is None:
                dframe = df_GIS[cols_geojson].iloc[n: n+1]
            else:
                dframe = df_GIS[cols_geojson][condition]
            gdframe = gpd.GeoDataFrame(dframe, crs = {'init': 'epsg:4326'}, geometry = 'geom_' + 'schzone_' + school)
            gsrating = gdframe['gsrating_' + school].iloc[0]

#             if 'primary' in geom:
#                 gdframe['style'] = [{'fillColor': '#d6c424', 'weight': .5, 'color': 'black'}]
#             elif 'middle' in geom:
#                 gdframe['style'] = [{'fillColor': '#24d6d6', 'weight': .5, 'color': 'black'}]
#             elif 'high' in geom:
#                 gdframe['style'] = [{'fillColor': '#ce2d3d', 'weight': .5, 'color': 'black'}]


            if gsrating in range(0, 4):
                schoolColor = 'red'  
                gdframe['style'] = [{'fillColor': '#cc3030', 'weight': .5, 'color': 'black'}]
            elif gsrating in range(4, 7):
                schoolColor = 'orange'  
                gdframe['style'] = [{'fillColor': '#e2c541', 'weight': .5, 'color': 'black'}]
            elif gsrating in range(7, 10):
                schoolColor = 'green'  
                gdframe['style'] = [{'fillColor': '#5dc611', 'weight': .5, 'color': 'black'}]  
            else:
                schoolColor = 'lightgray'
                gdframe['style'] = [{'fillColor': '#535556', 'weight': .5, 'color': 'black'}] 
                
            centroid = Point(gdframe['lng_school_' + school].iloc[0], gdframe['lat_school_' + school].iloc[0])
            popdict = dict(gdframe[cols_popup].iloc[0])
            geodict[school] = {'gdf': gdframe
                               ,'centroid': centroid
                               ,'popup': {school_trans(key, school) : popdict[key] for key in popdict}
                               ,'icon_color': schoolColor
                               }
    return geodict, house



def map_AllFeatures_ForAHouse(df_GIS, n=0, zipcode=None, address=None, city=None, zoom_start=11, saveTo=None, saveName=None):
    """
    map all the geometry features, centroid and popup information for a single house
    ------------------
    - n: the nth house in dataframe df_GIS
    """
    geodict, house = get_GISFeaturesForHouse(df_GIS, n=n, zipcode=zipcode, address=address, city=city)
    dist_line_color = 'purple'

    # set map center
    center_map = list(house[['lat', 'long']])

    # set up base map
    map_a_house = folium.Map(location=center_map, tiles=None, zoom_start=zoom_start)

    # set up layers
    lyr1 = folium.FeatureGroup('home')
    lyr2 = folium.FeatureGroup('amenities centroid')
    lyr3 = folium.FeatureGroup('amenities geometry')
    lyr4 = folium.FeatureGroup('distance line')
    lyr_prmr = folium.FeatureGroup('primary school')
    lyr_midl = folium.FeatureGroup('middle school')
    lyr_high = folium.FeatureGroup('high school')

    # highlight home
    param = dict(house)
    iframe = branca.element.IFrame(html_home(**param), width=250, height=150)
    pop_home = folium.Popup(iframe, max_width=2650)  
    icon_home = folium.Icon(icon='home', color='pink')
    folium.Marker(location=(float(house['lat']), float(house['long'])), 
                  popup= pop_home,
                  icon=icon_home).add_to(lyr1)


    # highlight amenity boundary and centroid
    for amenity in geodict:
        school_labels = ['primary', 'middle', 'high']
        
        # set up popup icons
        icon_info = folium.Icon(icon='info-sign')
        if amenity in school_labels:
            icon_school = folium.Icon(icon='graduation-cap', prefix='fa', color=geodict[amenity]['icon_color']) 
        icon_trash = folium.Icon(icon='trash', color='black')
        icon_hospital = folium.Icon(icon='plus-square', prefix='fa', color='red')

        # create popup text for each amenity
        param = geodict[amenity]['popup']
        param['type'] = amenity
        if amenity not in school_labels:
            iframe = branca.element.IFrame(html_GIS(**param), width=250, height=150)
        else:
            iframe = branca.element.IFrame(html_school(**param), width=250, height=220)
        pop_amnt = folium.Popup(iframe, max_width=2650)        

        # add schools and other amenities to different layers
        centroid = geodict[amenity]['centroid']
        marker = folium.Marker(location=(centroid.y, centroid.x), 
                               popup= pop_amnt, 
                               icon=icon_school if amenity in school_labels else icon_info)
        if amenity not in school_labels:
            marker.add_to(lyr2)
        elif amenity == 'primary':
            marker.add_to(lyr_prmr)
        elif amenity == 'middle':
            marker.add_to(lyr_midl)
        elif amenity == 'high':
            marker.add_to(lyr_high)


        # plot geometry unless it's a point
        if type(geodict[amenity]['gdf'].geometry.iloc[0]) != shapely.geometry.point.Point:
            geojson = folium.GeoJson(geodict[amenity]['gdf'])
            if amenity not in school_labels:
                geojson.add_to(lyr3)
            elif amenity == 'primary':
                geojson.add_to(lyr_prmr)
            elif amenity == 'middle':
                geojson.add_to(lyr_midl)
            elif amenity == 'high':
                geojson.add_to(lyr_high)

        # create distance line between home and amenities
        location = [center_map, [centroid.y, centroid.x]]
        dist_line = folium.PolyLine(locations=location, color=dist_line_color, weight=2, opacity = 0.5)
        if amenity not in school_labels:
            dist_line.add_to(lyr4)
        elif amenity == 'primary':
            dist_line.add_to(lyr_prmr)
        elif amenity == 'middle':
            dist_line.add_to(lyr_midl)
        elif amenity == 'high':
            dist_line.add_to(lyr_high)


    # render the layers
    folium.TileLayer('OpenStreetMap').add_to(map_a_house)
    folium.TileLayer('stamentoner').add_to(map_a_house)
    folium.TileLayer('Stamen Terrain').add_to(map_a_house)
    folium.TileLayer('CartoDBpositron').add_to(map_a_house)
    # folium.TileLayer('Mapbox', API_key=token_mpbox, attr='mapbox').add_to(mapc)

    # folium.TileLayer(tiles='https://api.mapbox.com/v4/mapbox.satellite/3/2/3.jpg70?access_token=%s' % token_mpbox,
    #                  attr='mapbox').add_to(mapc)


    for layer in [lyr1, lyr2, lyr3, lyr4, lyr_prmr, lyr_midl, lyr_high]:
        layer.add_to(map_a_house)

    # folium.LatLngPopup().add_to(mapc)
    folium.plugins.ScrollZoomToggler().add_to(map_a_house)
    folium.LayerControl().add_to(map_a_house)

    if saveTo == None:
        return map_a_house
    else:
        map_a_house.save("{path}/{file}.html".format(path=saveTo, file=saveName))
        return IFrame("{path}/{file}.html".format(path=saveTo, file=saveName), width=1000, height=500)




# ### Plot all houses in selected school zones
def get_houses_forAschool(df, schName=None, schId=None):
    """
    return all houses in a school zone based on school name or ID
    -----------------
    -df: df_GIS table
    """
    # get school type 
    #     schoolid = '120039003345'
    cols_schname = ['schoolname_high', 'schoolname_middle', 'schoolname_primary']
    cols_schid = ['ncessch_high', 'ncessch_middle', 'ncessch_primary']
    if schId:
        schooltype = [col for col in cols_schid if schId in list(df[col])][0].split('_')[-1]
    if schName:
        schooltype = [col for col in cols_schname if schName in list(df[col])][0].split('_')[-1]
    
    # get right columns for house data
    cols_house = (['zip', 'address', 'city', 'lat', 'long'] + 
                    ['distance_school_' + schooltype])
    cond_name = df.apply(lambda row: schName in (row.schoolname_high, row.schoolname_middle, row.schoolname_primary), 
                        axis=1)
    
    cond_id = df.apply(lambda row: schId in (row.ncessch_high, row.ncessch_middle, row.ncessch_primary), 
                        axis=1)
    result = df[cond_name|cond_id][cols_house]
    result = result.assign(amenity='school_' + schooltype)
    result.rename(columns={'distance_school_' + schooltype: 'distance'}, inplace=True)
    return result


# get_houses_forAschool(df_GIS, schName=schName, schId=schId)


def get_school_list(df, schRateAbove=0, schRateBelow=10, schName=None, schID=None, 
                    schLevel=None, IDonly=None, dfonly=None):
    """
    get a list of school ids meeting certain conditions
    -----------------
    df: df_GIS like table
    """
    # create a school indicing dataframe
    school_levels = ['middle', 'primary', 'high']
    school_cols = ['ncessch_', 'gsrating_', 'schoolname_', 'geom_schzone_', 'lat_school_', 'lng_school_']
    school_list = []
    for level in school_levels:
        rating_list = df[[col + level for col in school_cols]].drop_duplicates(
            subset=['ncessch_' + level, 'gsrating_' + level])
        rating_list.rename(columns=lambda col: col.replace('_' + level, ''), inplace=True)
        rating_list['level'] = level
        school_list.append(rating_list)
    school_list_df = pd.concat(school_list, ignore_index=True)
    
    # conditioning on rating
    cond_rate = school_list_df['gsrating'].apply(lambda x: x in range(schRateAbove, schRateBelow + 1))
    
    # conditioning on level
    if schLevel == None:
        dftLevel = ['primary', 'middle', 'high']
    else:
        dftLevel = schLevel
    cond_level = school_list_df['level'].apply(lambda x: x in dftLevel) 
    
    # conditioning on name or id
    if schName == None:
        cond_name = pd.Series([True] * school_list_df.shape[0])
    else:
        schName = [schName] if type(schName) == str else schName
        cond_name = school_list_df['schoolname'].isin(schName)
        
    if schID == None:
        cond_id = pd.Series([True] * school_list_df.shape[0])
    else:
        schID = [schID] if type(schID) == str else schID
        cond_id = school_list_df['ncessch'].isin(schID)
    
    # combining the conditions above
    df_result = school_list_df[cond_rate & cond_level & cond_name & cond_id]
    id_result = list(df_result['ncessch'].unique())
    
    if IDonly:
        return id_result
    if dfonly:
        return df_result
    else:
        return id_result, df_result
    

def get_InSchoolZone_Houses(df, schRateAbove=0, schRateBelow=10, schName=None, schID=None, schLevel=None):
    """
    return schools meeting conditions and corresponding houses in those school zones
    ------------------
    df: df_GIS
    schRateAbove, schRateBelow: 1 - 10, int
    schLevel: 'primary', 'middle', 'high' or combined list
    """
    schoolID, schooldf = get_school_list(df, schRateAbove=schRateAbove, schRateBelow= schRateBelow, 
                                         schName=schName, schID=schID, schLevel=schLevel)
    geomapping = {}
    for ID in schoolID:
        dframe = schooldf[schooldf['ncessch'] == ID]
        gdframe = gpd.GeoDataFrame(dframe, crs = {'init': 'epsg:4326'}, geometry = 'geom_schzone') # geo Dataframe
        gsrating = gdframe['gsrating'].iloc[0]
        
         # get school zone color and icon color based on gsrating
        if gsrating in range(0, 4):
            schoolColor = 'red'  
            gdframe['style'] = [{'fillColor': '#cc3030', 'weight': .5, 'color': 'black'}]
        elif gsrating in range(4, 7):
            schoolColor = 'orange'  
            gdframe['style'] = [{'fillColor': '#e2c541', 'weight': .5, 'color': 'black'}]
        elif gsrating in range(7, 10):
            schoolColor = 'green'  
            gdframe['style'] = [{'fillColor': '#5dc611', 'weight': .5, 'color': 'black'}]  
        else:
            schoolColor = 'lightgray'
            gdframe['style'] = [{'fillColor': '#535556', 'weight': .5, 'color': 'black'}]
            
        centroid = Point(gdframe['lng_school'].iloc[0], gdframe['lat_school'].iloc[0])
        houses = get_houses_forAschool(df, schId=ID)
        popup = dict(gdframe.rename(columns={'level': 'type'}).drop('geom_schzone', axis=1).iloc[0])
        popup['distance_school'] = 'N/A'
    
        geomapping[ID] = {'gdf': gdframe 
                         ,'centroid': centroid
                         ,'popup': popup
                         ,'houses': houses
                         ,'icon_color': schoolColor
                         }
    return geomapping


def map_InSchoolZone_Houses(df_GIS, schRateAbove=3, schRateBelow=8, schName=None, schID=None, schLevel=None,
                            radius = 0.5, zoom_start = 11, saveTo=None, saveName=None):
    geomapping = get_InSchoolZone_Houses(df=df_GIS, schRateAbove=schRateAbove, schRateBelow=schRateBelow, 
                                         schName=schName, schID=schID, schLevel=schLevel)
    cols_house = ['zip', 'address', 'city', 'lat', 'long']
    # houses = df_GIS[cols_house]
    # radius = 0.5
    # zoom_start = 11

    # set map center
    center_map = list(df_GIS[['lat', 'long']].mean())

    # set up base map
    map_schoolzone = folium.Map(location=center_map, tiles=None, zoom_start=zoom_start)

    for ID in geomapping:
    #     ID = '120039000607'
        schoolname = geomapping[ID]['gdf']['schoolname'].iloc[0]

        # set up layer for each school
        lyr = folium.FeatureGroup(schoolname)

        # set pop up icons
        icon_home = folium.Icon(icon='home')
        icon_school = folium.Icon(icon='graduation-cap', prefix='fa', color=geomapping[ID]['icon_color'])   


        # highlight school
        centroid = geomapping[ID]['centroid']
        param = geomapping[ID]['popup']
        iframe = branca.element.IFrame(html_school(**param), width=250, height=220)
        popup = folium.Popup(iframe, max_width=2650)
        folium.Marker(location=(centroid.y, centroid.x)
                     ,popup=popup
                     ,icon=icon_school
                     ).add_to(lyr)

        # highlight school zone
        geojson = folium.GeoJson(geomapping[ID]['gdf'])
        geojson.add_to(lyr)

        # highlight houses
        houses = geomapping[ID]['houses']
        for i in range(min(houses.shape[0], 100)):
            house = houses.iloc[i]
            param = dict(house)
            iframe = branca.element.IFrame(html_home_distance(**param), width=250, height=150)
            pop_home = folium.Popup(iframe, max_width=2650) 
            folium.CircleMarker(location=(float(house['lat']), float(house['long'])), 
                            radius=radius, color='red', fill_color='red', 
                            popup=pop_home
                           ).add_to(lyr)

        # add layer to map
        lyr.add_to(map_schoolzone)


    # render the layers
    folium.TileLayer('OpenStreetMap').add_to(map_schoolzone)
    folium.TileLayer('stamentoner').add_to(map_schoolzone)
    folium.TileLayer('Stamen Terrain').add_to(map_schoolzone)
    folium.TileLayer('CartoDBpositron').add_to(map_schoolzone)
    # folium.TileLayer('Mapbox', API_key=token_mpbox, attr='mapbox').add_to(mapc)
    # folium.TileLayer(tiles='https://api.mapbox.com/v4/mapbox.satellite/3/2/3.jpg70?access_token=%s' % token_mpbox,
    #                  attr='mapbox').add_to(mapc)

    # folium.LatLngPopup().add_to(map_schoolzone)
    folium.plugins.ScrollZoomToggler().add_to(map_schoolzone)
    folium.LayerControl().add_to(map_schoolzone)

    if saveTo == None:
        return map_schoolzone
    else:
        map_schoolzone.save("{path}/{file}.html".format(path=saveTo, file=saveName))
        return IFrame("{path}/{file}.html".format(path=saveTo, file=saveName), width=1000, height=500)




def get_all_amenities(df_GIS):
    """
    return all amenities name from a df_GIS
    """
    amenities = ['_'.join(col.split('_')[1:]) for col in list(df_GIS.columns) 
                 if 'geom_' in col and 'schzone' not in col]
    return amenities
    # amenities



def get_houses_near_amnty(df_GIS, amenity, threshold):
#     amenity = 'power_line' # amenity need to follow the custom of osm_id_amenity
#     threshold = 100
    geodict = {}
    
    # filter down to all the rows and cols meeting the condition
    cols_house = ['zip', 'address', 'city', 'lat', 'long'] + ['distance_' + amenity]
    cols_amnt = [col for col in list(df_GIS.columns) if amenity in col and col != 'distance_' + amenity] 
    df_amnt = df_GIS[df_GIS['distance_' + amenity] <= threshold][cols_house + cols_amnt]
    gdf_amnt = gpd.GeoDataFrame(df_amnt, crs={'init' :'epsg:4326'}, geometry='geom_' + amenity)

    houses = gdf_amnt[cols_house]
    # modify and add variable naems for houses to fit into html parameters
    houses = houses.assign(amenity = amenity)
    houses.rename(columns={'distance_' + amenity : 'distance'}, inplace=True)

    amenities = gdf_amnt[cols_amnt]
    amenities = amenities.drop_duplicates(subset='osm_id_' + amenity)
    amenities['style'] = [{'fillColor': '#37cbe5', 'weight': .5, 'color': 'black'}] * amenities.shape[0]
    
    geodict['houses'] = houses
    geodict['amenities'] = amenities
    return geodict



def map_Houses_NearAmnty(df_GIS, amenity, threshold, radius=0.5, zoom_start=11, saveTo=None, saveName=None):
    # radius = 0.5
    # zoom_start = 11

    geodict = get_houses_near_amnty(df_GIS=df_GIS, amenity=amenity, threshold=threshold)
    houses = geodict['houses']
    amenities = geodict['amenities']

    # set map center
    center_map = list(houses[['lat', 'long']].mean())

    # set up base map
    map_amnty = folium.Map(location=center_map, tiles=None, zoom_start=zoom_start)

    # set up layers
    lyr_houses = folium.FeatureGroup("houese")
    lyr_amenities = folium.FeatureGroup(amenity)

    # highlight houses
    for i in range(min(houses.shape[0], 10000)):
        house = houses.iloc[i]
        param = dict(house)
        iframe = branca.element.IFrame(html_home_distance(**param), width=250, height=180)
        pop_home = folium.Popup(iframe, max_width=2650) 
        folium.CircleMarker(location=(float(house['lat']), float(house['long'])), 
                        radius=radius, color='red', fill_color='red', 
                        popup=pop_home
                       ).add_to(lyr_houses)


    geojson = folium.GeoJson(amenities)
    geojson.add_to(lyr_amenities)


    # add layers to the map
    lyr_houses.add_to(map_amnty)
    lyr_amenities.add_to(map_amnty)

    # render the layers
    folium.TileLayer('OpenStreetMap').add_to(map_amnty)
    folium.TileLayer('stamentoner').add_to(map_amnty)
    folium.TileLayer('Stamen Terrain').add_to(map_amnty)
    folium.TileLayer('CartoDBpositron').add_to(map_amnty)

    # folium.LatLngPopup().add_to(map_schoolzone)
    folium.plugins.ScrollZoomToggler().add_to(map_amnty)
    folium.LayerControl().add_to(map_amnty)

    if saveTo == None:
        return map_amnty
    else:
        map_amnty.save("{path}/{file}.html".format(path=saveTo, file=saveName))
        return IFrame("{path}/{file}.html".format(path=saveTo, file=saveName), width=1000, height=500)



# ## Reference:
# + GeoJSON: https://en.wikipedia.org/wiki/GeoJSON
# + Creating Web Maps in Python with GeoPandas and Folium: http://andrewgaidus.com/leaflet_webmaps_python/
# + GIS in python: http://www.data-analysis-in-python.org/t_gis.html#
# + Mapping in Python with geopandas & mplleaflet:http://darribas.org/gds15/content/labs/lab_03.html
# + Geographic Data Science online Course: http://darribas.org/gds15/index.html
# + University of Helsinki GIS in python course:https://automating-gis-processes.github.io/2016/index.html
# + GeoPandas docs: http://geopandas.org/#
# + Another mapping library in python other than folium -- mplleaflet: https://github.com/jwass/mplleaflet
# + Shapely -- the equivalent of PostGIS in python: http://toblerity.org/shapely/index.html
# + folium examples1: http://nbviewer.jupyter.org/github/python-visualization/folium/tree/master/examples/
# + folium examples2: http://nbviewer.jupyter.org/github/ocefpaf/folium_notebooks/tree/master/
# + folium markers options: https://github.com/lvoogdt/Leaflet.awesome-markers
# + Fixing Error in sitecustomize; set PYTHONVERBOSE for traceback: KeyError: 'PYTHONPATH': http://www.agapow.net/programming/python/verbose/
# + Fixing bug for OSError: Could not find libspatialindex_c library file : http://jspeis.com/installing-rtree-on-mac-os-x/
