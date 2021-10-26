import os
from pathlib import Path
import pandas as pd

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

def datetime_parse_timestamp(time_in_secs):
    return pd.to_datetime(time_in_secs, unit='ms')

binance_columns = ["Open time", "Open", "High", "Low", "Close", "Volume"]
selected_columns = range(0, 6)

start_dt_list = []
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

    monthly_file = monthly_file_list[0].replace(".zip", "")
    daily_file = daily_file_list[-1]
    daily_file_name_split = daily_file.split("-{}-".format(BINANCE_INTERVAL))
    merged_filename = monthly_file + "-until-" + daily_file_name_split[1]

    # # Debugging Use
    # print (merged_filename)
    # break

    df_list = []
    for monthly_file in monthly_file_list:
        monthly_single_file_path = Path(os.path.join(monthly_interval_file_path, monthly_file)).resolve()

        # Loading historical tick data
        df = pd.read_csv(monthly_single_file_path, index_col=0, parse_dates=True, date_parser=datetime_parse_timestamp,
                         header=None, names=binance_columns, usecols=selected_columns)
        df_list.append(df)

        # # Debugging Use
        # break

    for daily_file in daily_file_list:
        daily_single_file_path = Path(os.path.join(daily_interval_file_path, daily_file)).resolve()

        # Loading historical tick data
        df = pd.read_csv(daily_single_file_path, index_col=0, parse_dates=True, date_parser=datetime_parse_timestamp,
                         header=None, names=binance_columns, usecols=selected_columns)
        df_list.append(df)

        # # Debugging Use
        # break

    merged_df = pd.concat(df_list, axis=0, ignore_index=False)
    merged_df = merged_df.sort_index()

    # Create the directory
    merged_file_dir = Path(os.path.join(merge_dir, symbol_id, BINANCE_INTERVAL)).resolve()
    if not os.path.exists(merged_file_dir):
        Path(merged_file_dir).mkdir(parents=True, exist_ok=True)

    merged_file_path = os.path.join(merged_file_dir, merged_filename)

    merged_df.to_csv(merged_file_path, compression='zip')
    print ("INFO: Saved {}".format(merged_file_path))

    # # Debugging Use
    # break
