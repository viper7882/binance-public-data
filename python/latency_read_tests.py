import os
from pathlib import Path
import pandas as pd

import ray
import vaex
import dask.dataframe as dd

from pprint import pprint
from time import time as timer

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

for symbol_id in monthly_symbols:
    merged_interval_file_path = Path(os.path.join(merge_dir, symbol_id, BINANCE_INTERVAL)).resolve()
    merged_file_list = [f for f in os.listdir(merged_interval_file_path) if
                        os.path.isfile(os.path.join(merged_interval_file_path, f))]

    merged_filename = merged_file_list[0]

    # # Evaluate Vaex
    # merged_file_dir = Path(os.path.join(merge_dir, symbol_id, BINANCE_INTERVAL)).resolve()
    # merged_file_path = os.path.join(merged_file_dir, merged_filename)
    #
    # Convert to HDF5 file
    #df = vaex.from_csv(merged_file_path, convert=True, chunk_size=5_000_000)

    # Read from HDF5 file
    # hdf5_merged_file_path = os.path.join(merged_file_dir, merged_filename + ".hdf5")
    # df = vaex.open(hdf5_merged_file_path)

    # Write to dedicated HDF5 file
    # dedicated_hdf5_merged_file_path = os.path.join(merged_file_dir, merged_filename.replace(".csv", "") + ".hdf5")
    # df.export(dedicated_hdf5_merged_file_path)

    # # Read from dedicated HDF5 file
    # df = vaex.open(dedicated_hdf5_merged_file_path)
    # pprint(df.head())


    # Evaluate Dask
    merged_file_path = os.path.join(merged_interval_file_path, merged_filename)

    start = timer()
    df = pd.read_csv(merged_file_path)
    df['Open time'] = pd.to_datetime(df['Open time'])
    pprint(type(df))
    pprint(df.dtypes)
    # pprint(df.head())
    hours, minutes, seconds = get_time_diff(start)
    print("Pandas spent: {}h {}m {}s".format(hours, minutes, seconds))

    start = timer()
    df = dd.read_csv(merged_file_path, blocksize=None)
    df['Open time'] = dd.to_datetime(df['Open time'])

    pprint(type(df))
    pprint(df.dtypes)
    # pprint(df.head())
    hours, minutes, seconds = get_time_diff(start)
    print("Dask spent: {}h {}m {}s".format(hours, minutes, seconds))

    # Debugging Use
    break

