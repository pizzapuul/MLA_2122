import pandas as pd
import numpy as np
import os
import matplotlib as mpl
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point, Polygon
import time



# starting time
start = time.time()

#Preparation for plotting data over europe
fig, axes = plt.subplots(nrows=1, ncols=1)
world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
europe=world[world.continent=='Europe']
#Remove Russia and Iceland from map of Europe
europe=europe[(europe.name!='Russia') & (europe.name!='Iceland')]
# Create a custom polygon
polygon = Polygon([(-25,35), (40,35), (40,75),(-25,75)])
#Clip polygon from the map of Europe
europe=gpd.clip(europe, polygon)
europe.plot(color='grey', ax=axes, alpha=0.5, linewidth=0.2, edgecolor='black')
cmap = mpl.colors.LinearSegmentedColormap.from_list("", ["green","yellow","red"])


def plot_delta_timestamps(data):
    data = pd.DataFrame.dropna(data) #deleting rows which contain NAN values

    #generate lists of dates and days
    date_range = pd.date_range(start ='1-1-2020', end ='12-31-2020', freq ='24H')
    date_list = [str(d.date()) for d in date_range]
    day_list = []
    for i in range(1, 367):
        day_list.append(str(i))

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
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(-10000,30), 1, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(30,60), 2, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(60,900), 3, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps'].between(900,3600), 4, data['delta_timestamps'])
    data['delta_timestamps'] = np.where(data['delta_timestamps']>3600, 5, data['delta_timestamps'])

    #Plot delta_timestamps on europe map
    data.plot(ax=axes, column='delta_timestamps', marker="o", markersize=0.5, cmap=cmap, alpha=0.5)
    axes.set_title('delta_timestamps')
    axes.yaxis.set_visible(False)
    axes.xaxis.set_visible(False)


    return(data)

file_list = [] #append each file here
for file in sorted(os.listdir('C:/Users/BIE/Desktop/Python/MLA/MLA_2122/data')):
    chunk_list = []  #append each chunk df here 
    df_chunk = pd.read_csv('data/'+file, usecols = ['latitude',
                                                    'longitude',
                                                    'timestamp_transfer',
                                                    'timestamp_measure_position'
                                                    ], low_memory = True, chunksize=100000)
    # Each chunk is in df format
    for data in df_chunk:  
        print('loading chunk')
        # perform data filtering 
        chunk_plot = plot_delta_timestamps(data)
        # Once the data plotting for the chunk is done, append the chunk to chunk_list
        chunk_list.append(chunk_plot)
        
    # concat the list into dataframe 
    df_file = pd.concat(chunk_list)
    # Once the data plotting for the file is done, append the file to file_list
    file_list.append(df_file)
    print('chunk complete')

#concat the list of files into dataframe
df_all_files = pd.concat(file_list)
    
print(df_all_files)
# end time
end = time.time()

# total time taken
print(f"Runtime of the program is {end - start}")

plt.show()

