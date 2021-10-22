@echo off
REM Refer to https://data.binance.vision/?prefix=data/futures/um/monthly/

SET START_DATE=2020-08-01
SET END_DATE=2021-10-01

REM Sanity Test
REM USDC List
SET SHELL_CMD=python download-kline.py -s XRPUSDC XLMUSDC -i 1m -startDate %START_DATE% -endDate %END_DATE%
echo %SHELL_CMD%
%SHELL_CMD%
