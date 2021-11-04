import os
import datetime
from pathlib import Path

import dask
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
MONTHLY_DATE_FORMAT = "%Y-%m"

CSV_FILE_NAME_EXTENSION = ".csv"
JSON_FILE_NAME_EXTENSION = ".json"
ZIP_FILE_NAME_EXTENSION = ".zip"
PARQUET_FILE_NAME_EXTENSION = ".parquet"

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

binance_columns = ["Open time", "Open", "High", "Low", "Close", "Volume"]

def get_time_diff(start):
    prog_time_diff = timer() - start
    hours, rem = divmod(prog_time_diff, 3600)
    minutes, seconds = divmod(rem, 60)
    return hours, minutes, seconds

for symbol_id in monthly_symbols:
    merged_interval_file_path = Path(os.path.join(merge_dir, symbol_id, BINANCE_INTERVAL)).resolve()

    print("merged_interval_file_path:")
    pprint(merged_interval_file_path)

    merged_file_list = [f for f in os.listdir(merged_interval_file_path) if
                        f.endswith(ZIP_FILE_NAME_EXTENSION) and
                        os.path.isfile(os.path.join(merged_interval_file_path, f))]

    merged_filename = merged_file_list[0]
    print("merged_filename: {}".format(merged_filename))

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
    # start = timer()
    # df = pd.read_csv(merged_file_path)
    # df[binance_columns[0]] = pd.to_datetime(df[binance_columns[0]])
    # pprint(type(df))
    # pprint(df.dtypes)
    # # pprint(df.head())
    # hours, minutes, seconds = get_time_diff(start)
    # print("Pandas spent: {}h {}m {}s".format(hours, minutes, seconds))

    start = timer()

    picked_timeline_start_dt = datetime.datetime(2021, 4, 29, 0, 0)
    picked_timeline_start_dt_str = picked_timeline_start_dt.strftime(DEFAULT_DATE_FORMAT)
    # pprint(picked_timeline_start_dt_str)

    merged_filename_start_dt_str = merged_filename.split("-until-")[0].split("-{}-".format(BINANCE_INTERVAL))[1]
    print("merged_filename_start_dt_str: {}".format(merged_filename_start_dt_str))

    # Convert string to datetime
    zip_filename_start_dt = datetime.datetime.strptime(merged_filename_start_dt_str, MONTHLY_DATE_FORMAT)
    zip_filename_start_dt_str = zip_filename_start_dt.strftime(DEFAULT_DATE_FORMAT)

    print("zip_filename_start_dt_str: {}".format(zip_filename_start_dt_str))
    print("picked_timeline_start_dt_str: {}".format(picked_timeline_start_dt_str))

    # If there is difference between zip file and picked date
    if zip_filename_start_dt != picked_timeline_start_dt:
        parquet_filename = merged_filename.replace(ZIP_FILE_NAME_EXTENSION, PARQUET_FILE_NAME_EXTENSION)
        parquet_filename = parquet_filename.replace(merged_filename_start_dt_str, picked_timeline_start_dt_str)
        print("parquet_filename: {}".format(parquet_filename))

        parquet_file_path = os.path.join(merged_interval_file_path, parquet_filename)

        # Check if the PARQUET file is absent
        if ((os.path.isfile(parquet_file_path) == False) or (os.path.exists(parquet_file_path) == False)):
            print("INFO: {} file absent".format(parquet_file_path))
            merged_file_path = os.path.join(merged_interval_file_path, merged_filename)

            print("merged_file_path:")
            pprint(merged_file_path)

            ddf = dd.read_csv(merged_file_path, blocksize=None)
            ddf[binance_columns[0]] = dd.to_datetime(ddf[binance_columns[0]])
            ddf = ddf.set_index(binance_columns[0], sorted=True)

            # df_open = ddf.iloc[:, [0]].compute()
            # df_open.to_parquet(parquet_file_path)
            # print("INFO: --> Saved {}".format(parquet_file_path))
        else:
            print("INFO: {} file present".format(parquet_file_path))
            ddf = dd.read_parquet(parquet_file_path)
            # ddf[binance_columns[0]] = dd.to_datetime(ddf[binance_columns[0]])
            # ddf = ddf.set_index(binance_columns[0], sorted=True)

    # print("ddf.dtypes:")
    # pprint(ddf.dtypes)

    print("ddf.head():")
    pprint(ddf.head())

    # NotImplementedError: 'DataFrame.iloc' only supports selecting columns. It must be used like 'df.iloc[:, column_indexer]'.
    # print("ddf.iloc[0]:")
    # pprint(ddf.iloc[0])

    # TypeError: '<' not supported between instances of 'int' and 'Timestamp'
    # print("ddf.loc[0]:")
    # pprint(ddf.loc[0])

    # AttributeError: 'DataFrame' object has no attribute 'name'
    # print("ddf.name:")
    # pprint(ddf.name)

    # first_row_dt = ddf.loc[0, binance_columns[0]].compute().to_frame().iloc[0][binance_columns[0]]
    # last_row_dt = ddf.loc[len(ddf.index) - 1, binance_columns[0]].compute().to_frame().iloc[0][binance_columns[0]]

    # first_row_dt_series, last_row_dt_series = dask.compute(
    #     ddf.loc[0],
    #     ddf.loc[len(ddf.index) - 1],
    # )
    # first_row_dt = first_row_dt_series.to_frame().iloc[0][binance_columns[0]]
    # last_row_dt = last_row_dt_series.to_frame().iloc[0][binance_columns[0]]

    first_row_dt = ddf.head(1).iloc[0].name
    last_row_dt = ddf.tail(1).iloc[0].name

    # pprint(type(first_row))
    # pprint(first_row.dtypes)
    pprint(first_row_dt.strftime(DEFAULT_DATE_TIME_FORMAT))

    # pprint(type(last_row))
    # pprint(last_row.dtypes)
    pprint(last_row_dt.strftime(DEFAULT_DATE_TIME_FORMAT))

    # # print("ddf:")
    # # pprint(ddf)
    # print("ddf.index: {}".format(ddf.index))
    #
    # ddf_in_range = ddf.loc[picked_timeline_start_dt.strftime(DEFAULT_DATE_TIME_FORMAT):
    #                        last_row_dt.strftime(DEFAULT_DATE_TIME_FORMAT)]
    # print("ddf_in_range:")
    # pprint(ddf_in_range)
    #
    # print("len(ddf.index):")
    # pprint(len(ddf.index))
    #
    # print("len(ddf_in_range.index):")
    # pprint(len(ddf_in_range.index))
    #
    # print("ddf_in_range.head(1):")
    # pprint(ddf_in_range.head(1))
    #
    # open_price = ddf_in_range.head(1)[binance_columns[1]].iloc[0]
    # print("open_price: {}".format(open_price))
    #
    # #print(type(ddf.index))
    #
    # #print(len(ddf_in_range.index))
    #
    # # start_dt = dd.to_datetime(first_row, format=DEFAULT_DATE_TIME_FORMAT)
    # # end_dt = dd.to_datetime(last_row, format=DEFAULT_DATE_TIME_FORMAT)
    # #
    # # pprint(start_dt.compute())
    # # pprint(end_dt.compute())
    #
    # # print("first_row: {}".format(first_row.compute().strftime(DEFAULT_DATE_TIME_FORMAT)))
    # # print("last_row: {}".format(last_row.compute().strftime(DEFAULT_DATE_TIME_FORMAT)))
    #
    # df_open = ddf.iloc[:, [0]].compute()
    # for i, index in enumerate(df_open.index):
    #     print ("index: {}".format(index))
    #     if i > 2:
    #         break
    #     else:
    #         i += 1


    hours, minutes, seconds = get_time_diff(start)
    print("Dask spent: {}:{}:{:.2f}s".format(int(hours), int(minutes), seconds))

    # Debugging Use
    break

