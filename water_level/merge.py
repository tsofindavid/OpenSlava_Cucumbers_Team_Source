import os
import pandas as pd
import pandas_gbq
from utils import init_pandas_gbq
from datetime import datetime 


init_pandas_gbq()


if __name__ == '__main__':
    stations_df = pd.read_csv("./seeds/current_water_level_res.csv")
    stations_df['station'] = stations_df['station_name'].apply(lambda x: x.replace('/', '_').replace(' ', '_').replace('(', '').replace(')', ''))
    pandas_gbq.to_gbq(stations_df, "climathon.RiverStations", if_exists='replace')

    dir_path = './data/water_level'

    # List all CSV files in the directory
    file_names = [f for f in os.listdir(dir_path) if f.endswith('.csv')]

    # Initialize an empty list to store DataFrames
    dfs = []

    # Iterate over each file, read it into a DataFrame, and append to the list
    for file_name in file_names:
        parts = file_name.split('_data_')
        station = parts[0]

        date_parts = parts[1].replace('.csv', '').split('_')
        
        start_date = datetime.strptime(date_parts[0], "%d.%m.%Y").date()
        end_date = datetime.strptime(date_parts[1], "%d.%m.%Y").date()
        
        # Read CSV file into a DataFrame
        df = pd.read_csv(os.path.join(dir_path, file_name))
        
        # Add station, start_date, and end_date columns
        df['station'] = station
        df['start_date'] = start_date
        df['end_date'] = end_date
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        
        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate all DataFrames into one
    merged_df = pd.concat(dfs, ignore_index=True)

    pandas_gbq.to_gbq(merged_df, "climathon.RiverStationsWaterLevel", if_exists='replace')