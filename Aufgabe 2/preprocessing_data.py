import pandas as pd
#from pandas.core.frame import DataFrame
import os
import matplotlib.pyplot as plt
import geopandas as gpd

   
for file in sorted(os.listdir('C:/Users/BIE/Desktop/Python/MLA/MLA_2122/data')):
    data = pd.read_csv('data/'+file)
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

# initialize an axis
fig, ax = plt.subplots(figsize=(8,6))
# plot map on axis
countries = gpd.read_file(  
     gpd.datasets.get_path("naturalearth_lowres"))
countries[countries["name"] == "Australia"].plot(color="lightgrey",
                                                 ax=ax)
# parse dates for plot's title
first_month = df["acq_date"].min().strftime("%b %Y")
last_month = df["acq_date"].max().strftime("%b %Y")
# plot points
df.plot(x="longitude", y="latitude", kind="scatter", 
        c="brightness", colormap="YlOrRd", 
        title=f"Fires in Australia {first_month} to {last_month}", 
        ax=ax)
# add grid
ax.grid(b=True, alpha=0.5)
plt.show()
