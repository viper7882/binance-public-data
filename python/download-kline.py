#!/usr/bin/env python

"""
  script to download klines.
  set the absoluate path destination folder for STORE_DIRECTORY, and run

  e.g. STORE_DIRECTORY=/data/ ./download-kline.py

"""
import sys
from datetime import *
import pandas as pd
from enums import *
from utility import download_file, get_all_symbols, get_parser, get_start_end_date_objects, convert_to_date_object, \
    get_path


def download_monthly_klines(trading_type, symbols, num_symbols, intervals, years, months, start_date, end_date, folder,
                            checksum, downloaded_list):
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    print("Found {} symbols".format(num_symbols))

    # Reverse the dates in place
    years.reverse()
    months.reverse()

    for symbol, downloaded in zip(symbols, downloaded_list):
        # Skip if the daily didn't download anything
        if downloaded == False:
            continue

        print("[{}/{}] - start download monthly {} klines ".format(current + 1, num_symbols, symbol))
        for interval in intervals:
            for year in years:
                for month in months:
                    current_date = convert_to_date_object('{}-{}-01'.format(year, month))
                    if current_date >= start_date and current_date <= end_date:
                        path = get_path(trading_type, "klines", "monthly", symbol, interval)
                        file_name = "{}-{}-{}-{}.zip".format(symbol.upper(), interval, year, '{:02d}'.format(month))
                        download_file(path, file_name, date_range, folder)

                        if checksum == 1:
                            checksum_path = get_path(trading_type, "klines", "monthly", symbol, interval)
                            checksum_file_name = "{}-{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, year,
                                                                                   '{:02d}'.format(month))
                            download_file(checksum_path, checksum_file_name, date_range, folder)

        current += 1


def download_daily_klines(trading_type, symbols, num_symbols, intervals, dates, start_date, end_date, folder, checksum):
    downloaded_list = []
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    # Get valid intervals for daily
    intervals = list(set(intervals) & set(DAILY_INTERVALS))
    print("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        return_value = True
        print("[{}/{}] - start download daily {} klines ".format(current + 1, num_symbols, symbol))
        for interval in intervals:
            for date in dates:
                current_date = convert_to_date_object(date)
                if current_date >= start_date and current_date <= end_date:
                    path = get_path(trading_type, "klines", "daily", symbol, interval)
                    file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, date)
                    return_value = download_file(path, file_name, date_range, folder)

                    if checksum == 1:
                        checksum_path = get_path(trading_type, "klines", "daily", symbol, interval)
                        checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, date)
                        return_value = download_file(checksum_path, checksum_file_name, date_range, folder)

                    downloaded_list.append(return_value)

                    if return_value == False:
                        break

        current += 1
    return downloaded_list


if __name__ == "__main__":
    parser = get_parser('klines')
    args = parser.parse_args(sys.argv[1:])

    if not args.symbols:
        print("fetching all symbols from exchange")
        symbols = get_all_symbols(args.type)
        num_symbols = len(symbols)
    else:
        symbols = args.symbols
        num_symbols = len(symbols)

    if args.dates:
        dates = args.dates
    else:
        long_dates = pd.date_range(end=datetime.today(), periods=MAX_DAYS).to_pydatetime().tolist()

        end_date_dt = datetime(END_DATE.year, END_DATE.month, 1)
        # Skip those dates that already covered by monthly klines
        reduced_dates = [date for date in long_dates if date > end_date_dt]

        dates = [date.strftime("%Y-%m-%d") for date in reduced_dates]
        # Reverse the dates in place
        dates.reverse()
        # download_monthly_klines(args.type, symbols, num_symbols, args.intervals, args.years, args.months, args.startDate, args.endDate, args.folder, args.checksum)
    downloaded_list = download_daily_klines(args.type, symbols, num_symbols, args.intervals, dates, args.startDate,
                                            args.endDate, args.folder, args.checksum)
    if not args.dates:
      download_monthly_klines(args.type, symbols, num_symbols, args.intervals, args.years, args.months, args.startDate, args.endDate, args.folder, args.checksum, downloaded_list)
