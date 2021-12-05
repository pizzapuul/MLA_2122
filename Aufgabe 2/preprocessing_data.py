import pandas as pd
import numpy as np
import os
import matplotlib as mpl
import matplotlib.pyplot as plt
import geopandas as gpd
import missingno as msn
from shapely.geometry import Point, Polygon

#Preparation for plotting data over europe
fig, ax = plt.subplots(figsize=(8,6))
world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
europe=world[world.continent=='Europe']
#Remove Russia and Iceland from map of Europe
europe=europe[(europe.name!='Russia') & (europe.name!='Iceland')]
# Create a custom polygon
polygon = Polygon([(-25,35), (40,35), (40,75),(-25,75)])
#Clip polygon from the map of Europe
europe=gpd.clip(europe, polygon)
base = europe.plot(color='#3B3C6E', ax=ax)
   
for file in sorted(os.listdir('C:/Users/BIE/Desktop/Python/MLA/MLA_2122/data')):
    data = pd.read_csv('data/'+file)
    #msn.bar(data, color='darkolivegreen') #checking on missing values
    data = pd.DataFrame.dropna(data) #deleting rows which contain NAN values 
    #data = data[data['movement_state'].notna()] #deleting rows which contain NAN values in specific columns
    #data=data.replace(to_replace=['parking', 'standing', 'moving'], value=[1, 2, 3]) #replacing strings with int
    #data=data.replace(to_replace=['Leer', 'Beladen'], value=[0, 1]) #replacing strings with int
    data['timestamp_measure_movement_state'] = data['timestamp_measure_movement_state'].str[:16] #removing unwanted characters
    data['timestamp_index'] = data['timestamp_index'].str[:16] #removing unwanted characters


    #change values to actual timestamps
    date_range = pd.date_range(start ='1-1-2020', end ='12-31-2020', freq ='24H')
    date_list = [str(d.date()) for d in date_range]
    day_list = []
    for i in range(1, 367):
        day_list.append(str(i))

    #for timestamp_index
    timestamp_index_date= data.timestamp_index.str[:2] #Just the Days of the Timestamp
    timestamp_index_date = timestamp_index_date.replace(to_replace=day_list, value=date_list) #change Days to actual date e.g. 42 days -> 2020-02-11
    timestamp_index_hours = data.timestamp_index.str[-8:] #Just the Time of the Timestamp
    data.timestamp_index =timestamp_index_date +" "+timestamp_index_hours #Combine Days and Hours in one column
    data['timestamp_index'] = pd.to_datetime(data.timestamp_index) #Convert to actual timestamps

    #for timestamp_measure_movement_state
    timestamp_measure_movement_state_date= data.timestamp_measure_movement_state.str[:2] #Just the Days of the Timestamp
    timestamp_measure_movement_state_date = timestamp_measure_movement_state_date.replace(to_replace=day_list, value=date_list) #change Days to actual date e.g. 42 days -> 2020-02-11
    timestamp_measure_movement_state_hours = data.timestamp_measure_movement_state.str[-8:] #Just the Time of the Timestamp
    data.timestamp_measure_movement_state =timestamp_measure_movement_state_date +" "+timestamp_measure_movement_state_hours #Combine Days and Hours in one column
    data['timestamp_measure_movement_state'] = pd.to_datetime(data.timestamp_measure_movement_state) #Convert to actual timestamps

    #add coloumn with delta timestamp in seconds
    data['delta_timestamps'] = (data.timestamp_index - data.timestamp_measure_movement_state).dt.total_seconds()

    # Change the coordinates to geoPoints
    data['coordinates'] = data[['longitude', 'latitude']].values.tolist()
    data['coordinates'] = data['coordinates'].apply(Point)
    data = gpd.GeoDataFrame(data, geometry='coordinates')

    #changing delta_timestamps from seconds to qualitative value
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(0,60), 1, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(60,300), 2, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(300,900), 3, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(900,3600), 4, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps']>3600, 5, data['delta_timestamps'])
    cmap = mpl.colors.LinearSegmentedColormap.from_list("", ["green","yellow","red"])
    data.plot(ax=base, column='delta_timestamps', marker="*", markersize=1, cmap=cmap, legend=True)

_ = ax.axis('off')
plt.show()


