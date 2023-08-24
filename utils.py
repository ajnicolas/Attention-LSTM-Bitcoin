import json
import os
import math
import logging
from datetime import datetime, timezone


def round_off(num, deci):
	return math.floor((num * pow(10, deci)) + 0.5) / pow(10, deci)

def truncate(f, n):
    '''Truncates/pads a float f to n decimal places without rounding'''
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(f, n)
    i, p, d = s.partition('.')
    return '.'.join([i, (d+'0'*n)[:n]])


logging.basicConfig(
    level=logging.INFO,
    filename='trade.log',
    filemode='w',
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

def utc_now():
	return datetime.now(timezone.utc)

def string_to_datetime(string):
	datetimeFormat = '%Y-%m-%d %H:%M'
	format = string[:16]

	return datetime.strptime(format, datetimeFormat)


def dhm(start, end):
	diff = string_to_datetime(end) - string_to_datetime(start)
	if(start > end):
		diff = string_to_datetime(start) - string_to_datetime(end)

	return int(diff.days), int(diff.seconds/3600), int((diff.seconds/60)%60)


def time_difference(timeframe, time_diff):

	if timeframe == "1m":
		count = time_diff[2] # Minutes already calculated
		count += (time_diff[0] * 24) * 60 # Get minutes in a day depending on how many days
		count += time_diff[1] * 60 # Get minutes in a hour depending on how many hours
		return int(count)

	elif timeframe == "5m":
		count = time_diff[2] # Minutes already calculated
		count += (time_diff[0] * 24) * 60 # Get minutes in a day depending on how many days
		count += time_diff[1] * 60 # Get minutes in a hour depending on how many hours
		return int(count/5)

	elif timeframe == "1h":
		count = time_diff[1] # hours already calculated
		count += time_diff[0] * 24 # Get minutes in a day depending on how many days
		count += time_diff[2] / 60 # Get minutes in a hour depending on how many hours
		return int(count)

	else:
		count = time_diff[0]
		return int(count)


def percentage_decrease(num1, num2):
	decrease = num1 - num2
	decrease = (decrease / num1) * 100

	return decrease

def percentage_increase(num1, num2):
	increase = num2 - num1
	increase = (increase / num1) * 100

	return increase

def Merge(dict1, dict2): 
    res = {**dict1, **dict2} 
    return res 


