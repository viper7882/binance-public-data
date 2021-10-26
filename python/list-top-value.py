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

BASE_DIR = Path("D:/Wecoz/github/binance-public-data/python/data/spot/daily/klines").resolve()
#pprint (BASE_DIR)
symbols = os.listdir(BASE_DIR)
#pprint (len(symbols))

def datetime_parse_timestamp(time_in_secs):
    return pd.to_datetime(time_in_secs, unit='ms')

binance_columns = ["Open time", "Open", "High", "Low", "Close", "Volume"]
selected_columns = range(0, 6)

top_value_list = []
for currency_pair in symbols:
    # Debugging Use
    # symbol_id = "BTCUSDC"

    INTERVAL_FILE_PATH = Path(os.path.join(BASE_DIR, currency_pair, "1m")).resolve()
    #pprint (INTERVAL_FILE_PATH)

    FILE_LIST = [f for f in os.listdir(INTERVAL_FILE_PATH) if os.path.isfile(os.path.join(INTERVAL_FILE_PATH, f))]
    #pprint (FILE_LIST)
    #pprint (FILE_LIST[0])
    # Sort in place
    #FILE_LIST.sort()

    df_list = []
    for file in FILE_LIST:
        SINGLE_FILE_PATH = Path(os.path.join(INTERVAL_FILE_PATH, file)).resolve()
        #pprint (SINGLE_FILE_PATH)

        # Loading historical tick data
        df = pd.read_csv(SINGLE_FILE_PATH, index_col=0, parse_dates=True, date_parser=datetime_parse_timestamp,
                         header=None, names=binance_columns, usecols=selected_columns)
        #pprint(df.head())
        # pprint(df.tail())

        # count_volume = df["Volume"].count()
        # pprint((SINGLE_FILE_PATH, count_volume))

        df_list.append(df)

    merged_df = pd.concat(df_list, axis=0, ignore_index=False)
    #merged_df = merged_df.sort_index()
    # pprint(merged_df.head())
    # pprint(merged_df.tail())

    # pprint(merged_df.iloc[0].name)
    # pprint(type(merged_df.iloc[0].name))

    top_value_df = merged_df["Volume"] * merged_df["Close"]
    top_value = top_value_df.sum()
    # pprint(top_value)
    # mean_volume = float(merged_df["Volume"].mean())
    # pprint(mean_volume)
    # count_volume = merged_df["Volume"].count()
    # pprint(count_volume)
    top_value_list.append((currency_pair, top_value))

    # Debugging Use
    # break

# Sort in place
top_value_list.sort(key=lambda x:x[1], reverse=True)

print("#    USDC        Value")
print("-" * 40)
for i, top_value_tuple in enumerate(top_value_list):
    currency_pair, top_value = top_value_tuple
    if currency_pair.endswith("USDC"):
        print("[{}]  {}     {}".format(i + 1, currency_pair, top_value))

print("")
print("#    USDT        Value")
print("-" * 40)
for i, top_value_tuple in enumerate(top_value_list):
    currency_pair, top_value = top_value_tuple
    if currency_pair.endswith("USDT"):
        print("[{}]  {}     {}".format(i + 1, currency_pair, top_value))
