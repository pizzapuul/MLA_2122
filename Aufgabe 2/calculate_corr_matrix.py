import pandas as pd
import os
import time
import dask.dataframe as dd




# starting time
start = time.time()

 #generate lists of dates and days
date_range = pd.date_range(start ='1-1-2020', end ='12-31-2020', freq ='24H')
date_list = [str(d.date()) for d in date_range]
day_list = []
for i in range(1, 367):
    day_list.append(str(i))

def calculate_corr_matrix(data):
    data = dd.DataFrame.dropna(data) #deleting rows which contain NAN values

    #for timestamp_transfer
    timestamp_transfer_date = data['timestamp_transfer'].str.split(' ').str[0] #Just the Days of the Timestamp
    timestamp_transfer_date = timestamp_transfer_date.replace(to_replace=day_list, value=date_list) #change Days to actual date e.g. 42 days -> 2020-02-11
    timestamp_transfer_hours = data['timestamp_transfer'].str.split(' ').str[2] #Just the Time of the Timestamp
    timestamp_transfer_hours = timestamp_transfer_hours.str[:8]
    data.timestamp_transfer =timestamp_transfer_date +" "+timestamp_transfer_hours #Combine Days and Hours in one column
    data['timestamp_transfer'] = dd.to_datetime(data.timestamp_transfer) #Convert to actual timestamps

    #for timestamp_measure_position
    timestamp_measure_position_date= data['timestamp_measure_position'].str.split(' ').str[0] #Just the Days of the Timestamp
    timestamp_measure_position_date = timestamp_measure_position_date.replace(to_replace=day_list, value=date_list) #change Days to actual date e.g. 42 days -> 2020-02-11
    timestamp_measure_position_hours = data['timestamp_measure_position'].str.split(' ').str[2] #Just the Time of the Timestamp
    timestamp_measure_position_hours = timestamp_measure_position_hours.str[:8]
    data.timestamp_measure_position =timestamp_measure_position_date +" "+timestamp_measure_position_hours #Combine Days and Hours in one column
    data['timestamp_measure_position'] = dd.to_datetime(data.timestamp_measure_position) #Convert to actual timestamps

    #add coloumn with delta timestamp in seconds
    data['delta_timestamps'] = (data.timestamp_transfer - data.timestamp_measure_position).dt.total_seconds()

    #changing delta_timestamps from seconds to qualitative value
    data.delta_timestamps = data['delta_timestamps'].mask(data['delta_timestamps']<30, 1)
    data.delta_timestamps = data['delta_timestamps'].mask(data['delta_timestamps'].between(30,60), 2)
    data.delta_timestamps = data['delta_timestamps'].mask(data['delta_timestamps'].between(61,600), 3)
    data.delta_timestamps = data['delta_timestamps'].mask(data['delta_timestamps'].between(601,3600), 4)
    data.delta_timestamps = data['delta_timestamps'].mask(data['delta_timestamps']>3601, 5)

    return(data)

file_list = [] #append each file here
for file in sorted(os.listdir('C:/Users/BIE/Desktop/Python/MLA/MLA_2122/data')): 
    data = dd.read_csv('data/'+file, usecols = ['timestamp_transfer',
                                                'timestamp_measure_position',
                                                'signal_quality_satellite',
                                                'signal_quality_hdop',
                                                'determination_position',
                                                'provider'
                                                ])
 
    file = calculate_corr_matrix(data)
    file_list.append(file)
df_file = dd.multi.concat(file_list)


corr = df_file.corr()
corr = corr.compute()
corr.drop('determination_position', axis=0, inplace=True)
corr.drop('determination_position', axis=1, inplace=True)
print(corr)

# end time
end = time.time()

# total time taken
print(f"Runtime of the program is {end - start}")


