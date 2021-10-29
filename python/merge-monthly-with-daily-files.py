import os
import copy
import datetime
import json
from pathlib import Path
import pandas as pd
import dask.dataframe as dd
from time import time as timer

from pprint import pprint

# Display all columns
pd.set_option('display.max_columns', None)

# Refer to https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
DEFAULT_DATE_FORMAT = "%Y-%m-%d"
DEFAULT_TIME_FORMAT = "%H:%M:%S.%f"
DEFAULT_DATE_TIME_FORMAT = DEFAULT_DATE_FORMAT + " " + DEFAULT_TIME_FORMAT

BINANCE_INTERVAL = "1m"
BINANCE_BASE_DIR = "D:/Wecoz/github/binance-public-data/python/data"
BINANCE_SPOT_MONTHLY_KLINES = BINANCE_BASE_DIR + "/spot/monthly/klines"
BINANCE_SPOT_DAILY_KLINES = BINANCE_BASE_DIR + "/spot/daily/klines"
BINANCE_SPOT_MERGED_KLINES = BINANCE_BASE_DIR + "/spot/merged/klines"
DEFAULT_TIMELINE_JSON_FILE_NAME = "default_timeline.json"

monthly_base_dir = Path(BINANCE_SPOT_MONTHLY_KLINES).resolve()
daily_base_dir = Path(BINANCE_SPOT_DAILY_KLINES).resolve()
merge_dir = Path(BINANCE_SPOT_MERGED_KLINES).resolve()
monthly_symbols = os.listdir(monthly_base_dir)
daily_symbols = os.listdir(monthly_base_dir)

assert monthly_symbols == daily_symbols

def get_time_diff(start):
    prog_time_diff = timer() - start
    hours, rem = divmod(prog_time_diff, 3600)
    minutes, seconds = divmod(rem, 60)
    return hours, minutes, seconds

def datetime_parse_timestamp(time_in_secs):
    return pd.to_datetime(time_in_secs, unit='ms')

binance_columns = ["Open time", "Open", "High", "Low", "Close", "Volume"]
selected_columns = range(0, 6)

overall_timeline_dict_template = {
    'data_min_dt_str'       : None,
    'data_max_dt_str'       : None,
    'specific_timeline_dict': None,     # To be filled with list of specific_timeline_dict
}

specific_timeline_dict_template = {
    'symbol_id'             : None,
    'first_row_dt_str'      : None,
    'total_indices'         : None,
    'last_row_dt_str'       : None,
    'open'                  : None,
}

overall_timeline_dict = copy.deepcopy(overall_timeline_dict_template)
overall_timeline_dict['specific_timeline_dict'] = []

data_min_dt = datetime.datetime.utcnow()
data_max_dt = datetime.datetime(1970, 1, 1)

start = timer()
for symbol_id in monthly_symbols:
    monthly_interval_file_path = Path(os.path.join(monthly_base_dir, symbol_id, BINANCE_INTERVAL)).resolve()
    monthly_file_list = [f for f in os.listdir(monthly_interval_file_path) if
                         os.path.isfile(os.path.join(monthly_interval_file_path, f))]

    daily_interval_file_path = Path(os.path.join(daily_base_dir, symbol_id, BINANCE_INTERVAL)).resolve()
    daily_file_list = [f for f in os.listdir(daily_interval_file_path) if
                       os.path.isfile(os.path.join(daily_interval_file_path, f))]

    # Sort in place
    monthly_file_list.sort()

    # Sort in place
    daily_file_list.sort()

    monthly_file_name = monthly_file_list[0].replace(".zip", "")
    daily_file = daily_file_list[-1]
    daily_file_name_split = daily_file.split("-{}-".format(BINANCE_INTERVAL))
    merged_filename = monthly_file_name + "-until-" + daily_file_name_split[1]

    # # Debugging Use
    # print (merged_filename)
    # break

    merged_file_dir = Path(os.path.join(merge_dir, symbol_id, BINANCE_INTERVAL)).resolve()
    merged_file_path = os.path.join(merged_file_dir, merged_filename)
    # If merged file path does not exist
    if not os.path.exists(merged_file_path):
        df_list = []
        for monthly_file in monthly_file_list:
            monthly_single_file_path = Path(os.path.join(monthly_interval_file_path, monthly_file)).resolve()

            # Loading historical tick data
            df = pd.read_csv(monthly_single_file_path, index_col=0, parse_dates=True,
                             date_parser=datetime_parse_timestamp, header=None, names=binance_columns,
                             usecols=selected_columns)
            df_list.append(df)

            # # Debugging Use
            # break

        for daily_file in daily_file_list:
            daily_single_file_path = Path(os.path.join(daily_interval_file_path, daily_file)).resolve()

            # Loading historical tick data
            df = pd.read_csv(daily_single_file_path, index_col=0, parse_dates=True,
                             date_parser=datetime_parse_timestamp, header=None, names=binance_columns,
                             usecols=selected_columns)
            df_list.append(df)

            # # Debugging Use
            # break

        merged_df = pd.concat(df_list, axis=0, ignore_index=False)
        merged_df = merged_df.sort_index()

        # Contents to be stored into JSON file
        open_price = float(merged_df[binance_columns[1]].iloc[0])
        first_row_dt = pd.to_datetime(merged_df.iloc[0].name)
        last_row_dt = pd.to_datetime(merged_df.iloc[-1].name)
        total_indices = len(merged_df.index)

        # Create the directory
        if not os.path.exists(merged_file_dir):
            Path(merged_file_dir).mkdir(parents=True, exist_ok=True)

        merged_df.to_csv(merged_file_path, compression='zip')
        print ("INFO: Saved {}".format(merged_file_path))
    # Else merged file path already exist
    else:
        print ("INFO: Found {}".format(merged_file_path))

        ddf = dd.read_csv(merged_file_path, blocksize=None)
        ddf[binance_columns[0]] = dd.to_datetime(ddf[binance_columns[0]])
        ddf = ddf.set_index(binance_columns[0], sorted=True)

        # Contents to be stored into JSON file
        first_row_dt = ddf.head(1).iloc[0].name
        last_row_dt = ddf.tail(1).iloc[0].name
        open_price = ddf.head(1)[binance_columns[1]].iloc[0]
        total_indices = len(ddf.index)

        # pprint(ddf.head())
        # pprint(first_row_dt.strftime(DEFAULT_DATE_TIME_FORMAT))
        # pprint(last_row_dt.strftime(DEFAULT_DATE_TIME_FORMAT))

    if first_row_dt < data_min_dt:
        data_min_dt = first_row_dt

    if first_row_dt > data_max_dt:
        data_max_dt = first_row_dt

    specific_timeline_dict = copy.deepcopy(specific_timeline_dict_template)
    specific_timeline_dict['symbol_id'] = symbol_id
    specific_timeline_dict['open'] = open_price
    specific_timeline_dict['first_row_dt_str'] = first_row_dt.strftime(DEFAULT_DATE_TIME_FORMAT)
    specific_timeline_dict['total_indices'] = total_indices
    specific_timeline_dict['last_row_dt_str'] = last_row_dt.strftime(DEFAULT_DATE_TIME_FORMAT)
    overall_timeline_dict['specific_timeline_dict'].append(specific_timeline_dict)

    # # Debugging Use
    # break

overall_timeline_dict['data_min_dt_str'] = data_min_dt.strftime(DEFAULT_DATE_TIME_FORMAT)
overall_timeline_dict['data_max_dt_str'] = data_max_dt.strftime(DEFAULT_DATE_TIME_FORMAT)

# pprint(overall_timeline_dict)
timeline_file_path = os.path.join(merge_dir, DEFAULT_TIMELINE_JSON_FILE_NAME)

with open(timeline_file_path, "w") as outfile:
    outfile.write("{}\n".format(json.dumps(overall_timeline_dict, indent=4, sort_keys=True).strip()))
    print("INFO: --> Saved {}".format(timeline_file_path))

hours, minutes, seconds = get_time_diff(start)
print("Spent: {}:{}:{:.2f}s".format(int(hours), int(minutes), seconds))
