# Findings: Dropped pyarrow due to its limitation where CSV must have header. Difficult to convert column 1 to datetime.

import zipfile
import pyarrow as pa
from pyarrow import csv
from pathlib import Path

from pprint import pprint

zipfile_path = Path("D:/Wecoz/github/binance-public-data/python/data/spot/daily/klines/BTCUSDC/1m/BTCUSDC-1m-2021-10-15.zip").resolve()

binance_columns = ["Open time", "Open", "High", "Low", "Close", "Volume"]
selected_columns = [i for i in range(0, 6)]

with zipfile.ZipFile(zipfile_path) as zip:
    with zip.open("BTCUSDC-1m-2021-10-15.csv") as myZip:
        convert_options = pa.csv.ConvertOptions()
        #convert_options.include_columns = selected_columns
        # convert_options.column_types = {
        #     binance_columns[0]: pa.date64(),
        #     # binance_columns[0]: pa.int64(),
        #     # binance_columns[1]: pa.double(),
        #     # binance_columns[2]: pa.double(),
        #     # binance_columns[3]: pa.double(),
        #     # binance_columns[4]: pa.double(),
        #     # binance_columns[5]: pa.double(),
        # }
        convert_options.timestamp_parsers = []
        table = csv.read_csv(myZip, convert_options=convert_options)
        pprint(table)