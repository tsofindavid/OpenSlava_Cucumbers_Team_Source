import os
import pandas as pd
import pandas_gbq
from datetime import datetime 

from utils import init_pandas_gbq

init_pandas_gbq()

# Assuming all files are stored in '/mnt/data' directory
dir_path = './data/weather'

if __name__ == '__main__':
    # List all CSV files in the directory
    file_names = [f for f in os.listdir(dir_path) if f.endswith('.csv')]

    # Initialize an empty list to store DataFrames
    dfs = []

    # Iterate over each file, read it into a DataFrame, and append to the list
    for file_name in file_names:
        station = file_name.replace('_weather_data.csv', '')
        df = pd.read_csv(os.path.join(dir_path, file_name))
        if 'time' not in df.columns:
            print(f'{file_name} is unprocessable')
            continue
        
        # Add station, start_date, and end_date columns
        df['station'] = station
        df['time'] = pd.to_datetime(df['time']).dt.date
        
        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate all DataFrames into one
    merged_df = pd.concat(dfs, ignore_index=True)

    pandas_gbq.to_gbq(merged_df, "climathon.RiverStationsWeather", if_exists='replace')