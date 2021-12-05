import pandas as pd
import numpy as np
import os
import matplotlib as mpl
import matplotlib.pyplot as plt
import geopandas as gpd
import missingno as msn
from shapely.geometry import Point, Polygon

#Preparation for plotting data over europe
fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(30,20))
world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
europe=world[world.continent=='Europe']
#Remove Russia and Iceland from map of Europe
europe=europe[(europe.name!='Russia') & (europe.name!='Iceland')]
# Create a custom polygon
polygon = Polygon([(-25,35), (40,35), (40,75),(-25,75)])
#Clip polygon from the map of Europe
europe=gpd.clip(europe, polygon)
europe.plot(color='#3B3C6E', ax=axes[0], alpha=0.8)
europe.plot(color='#3B3C6E', ax=axes[1], alpha=0.8)
# europe.plot(color='#3B3C6E', ax=axes[1,0], alpha=0.8)
# europe.plot(color='#3B3C6E', ax=axes[1,1], alpha=0.8)
cmap = mpl.colors.LinearSegmentedColormap.from_list("", ["green","yellow","red"])

#change values to actual timestamps
date_range = pd.date_range(start ='1-1-2020', end ='12-31-2020', freq ='24H')
date_list = [str(d.date()) for d in date_range]
day_list = []
for i in range(1, 367):
    day_list.append(str(i))

for file in sorted(os.listdir('C:/Users/BIE/Desktop/Python/MLA/MLA_2122/data')):
    data = pd.read_csv('data/'+file, usecols = ['latitude',
        	                                    'longitude',
                                                'timestamp_transfer',
                                                'timestamp_measure_position',
                                                'signal_quality_satellite',
                                                'signal_quality_hdop',
                                                'determination_position',
                                                'provider'
                                                ], low_memory = True)

    data = pd.DataFrame.dropna(data) #deleting rows which contain NAN values
    #data['timestamp_transfer'] = data['timestamp_transfer'].str[:16] #removing unwanted characters
    #data['timestamp_measure_position'] = data['timestamp_measure_position'].str[:16] #removing unwanted characters




    #for timestamp_transfer
    timestamp_transfer_date = data['timestamp_transfer'].str.split(' ').str[0] #Just the Days of the Timestamp
    timestamp_transfer_date = timestamp_transfer_date.replace(to_replace=day_list, value=date_list) #change Days to actual date e.g. 42 days -> 2020-02-11
    timestamp_transfer_hours = data['timestamp_transfer'].str.split(' ').str[2] #Just the Time of the Timestamp
    timestamp_transfer_hours = timestamp_transfer_hours.str[:8]
    data.timestamp_transfer =timestamp_transfer_date +" "+timestamp_transfer_hours #Combine Days and Hours in one column
    data['timestamp_transfer'] = pd.to_datetime(data.timestamp_transfer) #Convert to actual timestamps

    #for timestamp_measure_position
    timestamp_measure_position_date= data['timestamp_measure_position'].str.split(' ').str[0] #Just the Days of the Timestamp
    timestamp_measure_position_date = timestamp_measure_position_date.replace(to_replace=day_list, value=date_list) #change Days to actual date e.g. 42 days -> 2020-02-11
    timestamp_measure_position_hours = data['timestamp_measure_position'].str.split(' ').str[2] #Just the Time of the Timestamp
    timestamp_measure_position_hours = timestamp_measure_position_hours.str[:8]
    data.timestamp_measure_position =timestamp_measure_position_date +" "+timestamp_measure_position_hours #Combine Days and Hours in one column
    data['timestamp_measure_position'] = pd.to_datetime(data.timestamp_measure_position) #Convert to actual timestamps

    #add coloumn with delta timestamp in seconds
    data['delta_timestamps'] = (data.timestamp_transfer - data.timestamp_measure_position).dt.total_seconds()

    # Change the coordinates to geoPoints
    data['coordinates'] = data[['longitude', 'latitude']].values.tolist()
    data['coordinates'] = data['coordinates'].apply(Point)
    data = gpd.GeoDataFrame(data, geometry='coordinates')

    #changing delta_timestamps from seconds to qualitative value
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(-10000,60), 1, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(60,300), 2, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(300,900), 3, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(900,3600), 4, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps']>3600, 5, data['delta_timestamps'])

    #Plot delta_timestamps on europe map
    data.plot(ax=axes[0], column='delta_timestamps', marker="o", markersize=1, cmap=cmap, legend=True)
    axes[0].set_title('delta_timestamps')
    axes[0].yaxis.set_visible(False)
    axes[0].xaxis.set_visible(False)

    #Plot provider on europe map
    data.plot(ax=axes[1], column='provider', marker="o", markersize=10, cmap='cool', legend=True, alpha=0.1)
    axes[1].set_title('provider')
    axes[1].yaxis.set_visible(False)
    axes[1].xaxis.set_visible(False)

for i in range(0, 40):
    prov_i= data.loc[data['provider'] == i]
    print(prov_i['delta_timestamps'].mean(), prov_i['delta_timestamps'].var())
    
data_heatmap = data[['signal_quality_satellite',
                    'signal_quality_hdop',
                    'provider',
                    'delta_timestamps'
                    ]].copy()
   
corr = data_heatmap.corr()
print(corr)
print(data)
plt.show()




#data = data[data['movement_state'].notna()] #deleting rows which contain NAN values in specific columns
#data=data.replace(to_replace=['parking', 'standing', 'moving'], value=[1, 2, 3]) #replacing strings with int
#data=data.replace(to_replace=['Leer', 'Beladen'], value=[0, 1]) #replacing strings with int
#msn.bar(data, color='darkolivegreen') #checking on missing values