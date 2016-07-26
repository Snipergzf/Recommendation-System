# -*- coding:utf-8 -*-
# from pymongo import MongoClient
from pymongo import ASCENDING 
from pymongo import DESCENDING
from config import *
# import redis
import time
import datetime
import random
import json

class producer(object):
	def __init__(self, mongoConn, redisConn):
		# conn = MongoClient(MONGO_HOST, MONGO_PORT)
		# self.db = conn.core
		# self.db.authenticate(MONGO_USER, MONGO_PASSWD)
		# self.r = redis.StrictRedis(host="localhost", port=6379, db=0)
		self.db = mongoConn
		self.r = redisConn
		self.update_time = 0

	"""
	For every event in mongodb, put its brief into redis
	"""
	def post_events(self):
		cursor = self.db.cEvent.find({},
				{"_id":1, "cEvent_figure":1, "cEvent_theme":1,"cEvent_tag":1, "cEvent_name":1,"click_num":1}) 
		for event in cursor:
			# STRING_DATA = self.convert_unicode_to_str(event)
			self.r.set(event["_id"], json.dumps(event))
		# put the all sorted events id into redis
		id_list = self.db.cEvent.find({}, {"_id":1, "cEvent_publish":1}).sort("_id", DESCENDING)
		self.r.delete("all")
		self.r.delete("newEvents")
		for index, item in enumerate(id_list):
			self.r.zadd("all", float(index+0.5), item['_id'])
			if item['cEvent_publish'] > self.update_time:
				self.r.zadd("newEvents", float(index+0.5), item['_id'])


	"""
	Move the expired events to event_off collection
	"""
	def remove_expired(self):
		for event in self.db.cEvent.find():
			_len = len(event['cEvent_time'])
			if event['cEvent_time'][_len-1] <= int(time.time()):
				self.r.delete(event["_id"])
				self.db.cEvent_off.insert(event)
				self.db.cEvent.remove({"_id":event['_id']})

	def get_category(self):
		for i in CATEGORY_LIST:
			id_list = self.db.cEvent.find({"cEvent_theme":i}, {"_id":1}).sort("_id", DESCENDING)
			self.r.delete(i)
			for index, item in enumerate(id_list):
				self.r.zadd(i, float(index+0.5), item['_id'])


	def convert_unicode_to_str(self, data):
		result = {}
		for k, v in data.items():
			if isinstance(v, float):
				result[str(k)] = str(int(v))
			else:
				result[str(k)] = v.encode('utf8')
		return result

	def main(self):
		print("running at %s" % (datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
		self.post_events()
		self.remove_expired()
		self.get_category()
		print("finished at %s" % (datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
		self.update_time = int(time.time())
			

if __name__ == '__main__':
	pro = producer()
	while True:
		try:
			print("running at %s" % (datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
			pro.post_events()
			pro.remove_expired()
			pro.get_category()
			print("finished at %s" % (datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
			pro.update_time = int(time.time())
			time.sleep(INTERVAL+random.randint(-100,100))
		except KeyboardInterrupt:
			break
		
