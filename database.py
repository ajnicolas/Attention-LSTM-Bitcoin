import pandas as pd
import time, os
from sqlalchemy import create_engine, text
from utils import utc_now, dhm, time_difference, string_to_datetime
from math import pi


class SqlDatabase():
	"""
	Downloads candle data historically or by updating database to reach current candles. 
	"""

	def __init__(self, symbol, timeframe, autoUpdate=True):
		self.symbol = symbol
		self.timeframe = timeframe

		self.MAX_COUNT = 1000

		# Different dbs for different timeframes
		if timeframe == "1m":
			self.DB_NAME = "one_minute"
		elif timeframe == "5m":
			self.DB_NAME = "five_minute"
		elif timeframe == "1h":
			self.DB_NAME = "hourly"
		else:
			self.DB_NAME = "daily"

		self.DATABASE = 'sqlite:///./%s.db' % self.DB_NAME
		print("Using " + self.DB_NAME + ".db")

		if autoUpdate:
			self.update_db()



	def get_candles(self, count, start="", range=True, reverseData=True):
		"""
		Returns candle data depending parameters above from bitmex
		Returns pd.DataFrame from old data -> new data, appending new data
		to the end of the data

		Current data and so on, entire data set, certain date range dataset
		"""
		public_client = bitmex.bitmex(
			test = False
		)
		# Check here if start or end is empty, if not then range is true
		# if range is true then 

		if range:
			# Grabs historical dataset, reverse = False
			ohlcv_candles = pd.DataFrame(public_client.Trade.Trade_getBucketed(
				symbol = self.symbol,
				binSize = self.timeframe,
				startTime = start,
				count = count,
				reverse = reverseData,
				partial = False
			).result()[0])
		else:
			# Grabs current data, usually for starting dataset, reverse = True
			ohlcv_candles = pd.DataFrame(public_client.Trade.Trade_getBucketed(
				symbol = self.symbol,
				binSize = self.timeframe,
				count = count,
				reverse = reverseData,
				partial = False
			).result()[0])

		if reverseData:
			ohlcv_candles = ohlcv_candles.iloc[::-1].reset_index(drop=True)

		ohlcv_candles["timestamp"] = pd.to_datetime(ohlcv_candles["timestamp"])
		ohlcv_candles.set_index(['timestamp'], inplace=True)
		return ohlcv_candles



	def get_data(self,get_current=False):
		"""
		Kick starts data, default: Gets first 500 candles
		"""
		file = "./data/" + self.DB_NAME + ".db"

		if os.stat(file).st_size == 0:
			if get_current:
				# Gets the latest 500 candles
				ohlcv = self.get_candles(count=500,range=False,reverseData=True)
			else:
				# Gets first 500 candles of entire history
				ohlcv = self.get_candles(count=500,range=False,reverseData=False)

			engine = create_engine(self.DATABASE, echo=False)
			ohlcv.to_sql(self.DB_NAME, con=engine, if_exists='replace')
			print("Setting up database...")

		# else:
		# 	print(self.DB_NAME + ".db" + " already has data!")



	def render_chunks(self, reverseData=False):
		"""
		Renders new data either historically, or new data catch up
		"""

		last_timestamp = self.get_timestamp()["timestamp"].iloc[-1] # Retrieve latest item of dataframe
		diff = dhm(last_timestamp, str(utc_now()))
		count = time_difference(self.timeframe, diff) + 41
		total = count
		# 41286

		engine = create_engine(self.DATABASE, echo=False)
		while count > 0:
			# timestamp != str(utc_now())

			# Grab latest date from database and convert to datetime object
			start = string_to_datetime(self.get_timestamp()["timestamp"].iloc[-1])

			if count > 1000:
				# Downloads chunks of data to database
				ohlcv = self.get_candles(self.MAX_COUNT, start=start,reverseData=reverseData)
				count += -1000
			else:
				# Grab remainder chunk(last)
				ohlcv = self.get_candles(count, start=start,reverseData=reverseData)
				count += -count

			ohlcv.to_sql(self.DB_NAME, con=engine, if_exists='append')
			print("Download Progress:", str(int(100 - (count/total)*100))+"%")

		print("Download complete " + str(total) + "/" + str(total))


	def update_db(self):
		"""
		Update current data, already acknowledging there is data in database
		TODO: check if there is old timestamp to work with or call get_data first
		"""
		# Check to see if db has data already, if not then retrieve data
		self.get_data()


		last_timestamp = self.get_timestamp()["timestamp"].iloc[-1] # Retrieve first item of dataframe
		diff = dhm(last_timestamp, str(utc_now()))
		count = time_difference(self.timeframe, diff)
		print("Updating " + str(count) + " new candles")

		if count > 1000:
			self.render_chunks()
		else:
			# start = string_to_datetime(self.get_timestamp()["timestamp"].iloc[-1])

			if count != 0:
				ohlcv = self.get_candles(count, range=False)

				engine = create_engine(self.DATABASE, echo=False)
				ohlcv.to_sql(self.DB_NAME, con=engine, if_exists='append')
				print("Updated sql base")


	def delete_db(self):
		"""Deletes content in db (used for restarting db and clearing old memory) """
		file = "./data/" + self.DB_NAME + ".db"

		if os.stat(file).st_size != 0:
			open(file, 'w').close()
			print("Erased/deleted contents in " + file + "!")
		else:
			print(file + " is already empty!")


	# THIS PART IS DONE

	def access_db(self, column):
		"""
		Gives functions the ability to access sqlite3 database based on parameter
		"""
		engine = create_engine(self.DATABASE, echo=False)
		cnx = engine.connect()
		return pd.read_sql_query(text("SELECT %s FROM %s" % (column, self.DB_NAME)), cnx)


	def see_all(self):
		return self.access_db("*")

	def get_timestamp(self):
		return self.access_db("timestamp")

	def get_open(self):
		return self.access_db("open")

	def get_high(self):
		return self.access_db("high")

	def get_low(self):
		return self.access_db("low")

	def get_close(self):
		return self.access_db("close")

	def get_vol(self):
		return self.access_db("volume")

	def get_trades(self):
		return self.access_db("trades")

	def get_vwap(self):
		return self.access_db("vwap")

	def get_last_size(self):
		return self.access_db("lastsize")


	def get_ohlcv(self):
		ohlcv = self.access_db("timestamp, open, high, low, close, volume")
		# ohlcv["timestamp"] = pd.to_datetime(ohlcv["timestamp"])
		# ohlcv.set_index(['timestamp'], inplace=True)

		return ohlcv

	def ohlcv_range(self, columns, start, end):
		engine = create_engine(self.DATABASE, echo=False)
		cnx = engine.connect()

		# columns = "timestamp, open, high, low, close, volume"

		ohlcv = pd.read_sql_query(text("SELECT %s FROM %s WHERE timestamp BETWEEN '%s' AND '%s'" % (columns,self.DB_NAME, start, end)), cnx)
		if columns == "timestamp":
			ohlcv["timestamp"] = pd.to_datetime(ohlcv["timestamp"])

		return ohlcv



	def resample_(self, tf="3D"):
		"""Converts current timeframe data to a different timeframe"""

		ohlcv = self.ohlcv_range("2015-09-28", "2020-06-8")

		ohlcv.set_index(['timestamp'], inplace=True)

		return ohlcv.resample(tf).agg({
			"open": "first",
			"high": "max",
			"low": "min",
			"close": "last",
			"volume": "sum",
		})


	def get_database(self):
		return self.DATABASE

	def get_first(self):
		return self.access_db("timestamp")["timestamp"].iloc[0]

	def get_last(self):
		return self.access_db("timestamp")["timestamp"].iat[-1]


