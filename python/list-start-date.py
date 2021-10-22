import os
from pathlib import Path
import pandas as pd

from pprint import pprint

# Display all columns
pd.set_option('display.max_columns', None)

# Refer to https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
DEFAULT_DATE_FORMAT = "%Y-%m-%d"
DEFAULT_TIME_FORMAT = "%H:%M:%S.%f"
DEFAULT_DATE_TIME_FORMAT = DEFAULT_DATE_FORMAT + " " + DEFAULT_TIME_FORMAT

BASE_DIR = Path("D:/Wecoz/github/binance-public-data/python/data/spot/monthly/klines").resolve()
#pprint (BASE_DIR)
symbols = os.listdir(BASE_DIR)
#pprint (len(symbols))

def datetime_parse_timestamp(time_in_secs):
    return pd.to_datetime(time_in_secs, unit='ms')

binance_columns = ["Open time", "Open", "High", "Low", "Close", "Volume"]
selected_columns = range(0, 6)

start_dt_list = []
for currency_pair in symbols:
    # Debugging Use
    #currency_pair = "BTCUSDC"

    INTERVAL_FILE_PATH = Path(os.path.join(BASE_DIR, currency_pair, "1m")).resolve()
    #pprint (INTERVAL_FILE_PATH)

    FILE_LIST = [f for f in os.listdir(INTERVAL_FILE_PATH) if os.path.isfile(os.path.join(INTERVAL_FILE_PATH, f))]
    #pprint (FILE_LIST)
    #pprint (FILE_LIST[0])
    # Sort in place
    FILE_LIST.sort()

    df_list = []
    for file in FILE_LIST:
        SINGLE_FILE_PATH = Path(os.path.join(INTERVAL_FILE_PATH, file)).resolve()
        #pprint (SINGLE_FILE_PATH)

        # Loading historical tick data
        df = pd.read_csv(SINGLE_FILE_PATH, index_col=0, parse_dates=True, date_parser=datetime_parse_timestamp,
                         header=None, names=binance_columns, usecols=selected_columns)
        #pprint(df.head())
        # pprint(df.tail())
        df_list.append(df)

        # INFO: Only take the first sorted file
        break

    merged_df = pd.concat(df_list, axis=0, ignore_index=False)
    merged_df = merged_df.sort_index()
    #pprint(merged_df.head())
    # pprint(merged_df.tail())

    # pprint(merged_df.iloc[0].name)
    # pprint(type(merged_df.iloc[0].name))

    start_dt = pd.to_datetime(merged_df.iloc[0].name)
    start_dt_list.append((currency_pair, start_dt))

    # Debugging Use
    #break

# Sort in place
start_dt_list.sort(key=lambda x:x[1])

print("#    USDC        Start Datetime")
print("-" * 40)
for i, start_dt_tuple in enumerate(start_dt_list):
    currency_pair, start_dt = start_dt_tuple
    if currency_pair.endswith("USDC"):
        print("[{}]  {}     {}".format(i + 1, currency_pair, start_dt.strftime(DEFAULT_DATE_TIME_FORMAT)))

print("")
print("#    USDT        Start Datetime")
print("-" * 40)
for i, start_dt_tuple in enumerate(start_dt_list):
    currency_pair, start_dt = start_dt_tuple
    if currency_pair.endswith("USDT"):
        print("[{}]  {}     {}".format(i + 1, currency_pair, start_dt.strftime(DEFAULT_DATE_TIME_FORMAT)))
