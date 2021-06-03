# -*- coding: utf-8 -*-
# @Time    : 2020/12/21
# @Author  : Needn
# @Software: VS
import sys,os,uuid
from datetime import datetime, timedelta

# get a YYYYMM result after +/- [num] months
def add_month(year_month, num):
    month = year_month % 100 
    year_diff = int(num / 12)
    net_num = abs(num) % 12

    if num < 0:
        net_num *= -1

    if (net_num + month) > 12:
        net_num += 88

    if (net_num + month) < 1:
        net_num -= 88

    result_month = year_month + net_num + year_diff * 100
    return result_month

# constant
# ==================================================================================================================
UTC_NOW = datetime.utcnow()
NOW = UTC_NOW + timedelta(hours=8)
YESTERDAY_NOW = NOW - timedelta(days=1)

TODAY = NOW.strftime('%Y-%m-%d')

# all calendar related constant values are calculated based on YESTERDAY
YESTERDAY = YESTERDAY_NOW.strftime('%Y-%m-%d')
CURRENT_MONTH = YESTERDAY_NOW.strftime('%Y%m')
CURRENT_MONTH_1ST = YESTERDAY_NOW.strftime('%Y-%m-01')

LAST_MONTH = str(add_month(int(CURRENT_MONTH), -1))
LAST_MONTH_1ST = str(LAST_MONTH)[:4] + '-' + str(LAST_MONTH)[-2:] + '-01'

CURRENT_YEAR = YESTERDAY_NOW.strftime('%Y')
CURRENT_YEAR_1ST = YESTERDAY_NOW.strftime('%Y-01-01')

LAST_YEAR = str(int(CURRENT_YEAR) - 1)
LAST_YEAR_1ST = LAST_YEAR + '-01-01'

THREE_WEEKS_AGO = (YESTERDAY_NOW - timedelta(days=21)).strftime('%Y-%m-%d')
THREE_DAYS_AGO = (YESTERDAY_NOW - timedelta(days=3)).strftime('%Y-%m-%d')
ONE_WEEK_AGO = (NOW - timedelta(days=7)).strftime('%Y-%m-%d')
# =====================================================================================

def print_log(log_type, text):
    print(log_type + " " + str(datetime.utcnow() + timedelta(hours=8)) + " " + text)


# use UTC, Asia/Shanghai timezone only
def validate_timezone(timezone):
    if timezone not in ["Asia/Shanghai", "UTC"]:
        raise Exception("Can not process timezone [" + timezone + "]")

