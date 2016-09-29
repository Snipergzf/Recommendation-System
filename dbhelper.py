# -*- coding:utf-8 -*-
import MySQLdb
import redis
import time
from pymongo import MongoClient
from config import *
import re

class dbhelper(object):
	"""docstring for dbhelper"""
	def __init__(self):
		#create mysql connection
		self.mysql_conn = MySQLdb.connect("127.0.0.1", MYSQL_USER, MYSQL_PASSWD, "Register")
		#create mongo connection
		conn = MongoClient(MONGO_HOST, MONGO_PORT)
		self.mongo_conn = conn.core
		self.mongo_conn.authenticate(MONGO_USER, MONGO_PASSWD)
		#create redis connection
		self.redis_conn = redis.StrictRedis(host="localhost", port=6379, db=0)

	# def getMySQLConn(self):
	# 	return self.mysql_conn

	# def getMongoConn(self):
	# 	return self.mongo_conn

	# def getRedisConn(self):
	# 	return self.redis_conn
	
	
	# def getRecentUser(self):
		# cur = self.mysql_conn.cursor()
		# sql_str = "SELECT id FROM User"
		# n = cur.execute(sql_str)
		# if n==0:
			# cur.close()
			# return None
		# users = [i[0] for i in cur]
		# cur.close()
		# return users
		
	def getRecentUser(self):
		r = self.mongo_conn.cUser.find({},{"_id":1})
		# r = self.mongo_conn.cUser_test.find({},{"_id":1})
		rList = [i["_id"] for i in r]
		return rList
		
	def getUserRecord(self, uId):
		pastFeature = None
		recentRecord = []
		userfile = self.mongo_conn.cUser.find_one({"_id": uId})
		if "events" in userfile:
			recentRecord = [i for i in userfile['events'] if int(i['time']) > (int(time.time())-259200)]
		if "pastFeature" in userfile:
			pastFeature = userfile['pastFeature']
		if len(recentRecord) == 0:
			recentRecord = None
		return recentRecord, pastFeature

	def getUserFeture(self, uId):
		userfile = self.mongo_conn.cUser.find_one({"_id": uId})
		recentFeature = None
		pastFeature = None
		if "recentFeature" in userfile:
			#return user's recentFeature dict, including vector and time
			recentFeature = userfile['recentFeature']
		if "pastFeature" in userfile:
			#return user's pastFeature dict, including vector and time
			pastFeature = userfile['pastFeature']
		return recentFeature, pastFeature

	def getEventVector(self, eventId):
		event = self.mongo_conn.cEvent.find_one({"_id": eventId})
		# event = self.mongo_conn.cEvent_test.find_one({"_id": eventId})
		if event:
			return event['vector']
		else:
			return self.mongo_conn.cEvent_off.find_one({"_id": eventId})['vector']

	def putUserFeature(self, uId, re_feature, pa_feature):
		if re_feature is not None and pa_feature is not None:
			reFile = {"vector":re_feature, "time":int(time.time())}
			paFile = {"vector":pa_feature, "time":int(time.time())}
			result = self.mongo_conn.cUser.update({"_id": uId}, {"$set": {"pastFeature":paFile,"recentFeature":reFile}})
		elif pa_feature is not None and re_feature is None:
			paFile = {"vector":pa_feature, "time":int(time.time())}
			result = self.mongo_conn.cUser.update({"_id": uId}, {"$set": {"pastFeature":paFile}})
		elif re_feature is not None and pa_feature is None:
			reFile = {"vector":re_feature, "time":int(time.time())}
			result = self.mongo_conn.cUser.update({"_id": uId}, {"$set": {"recentFeature":reFile}})
		if result == 1:
			return True
		else:
			return False


	def getEventByClass(self, className):
		return (self.redis_conn.zrange(className, 0 ,-1))
 
	#remain to consider
	def getEventClassVector(self):
		stringList = (self.redis_conn.zrange("ClassVector", 0, -1))
		res = []
		for string in stringList:
			string_tmp = re.sub(r"[\[\]]",'',string)
			res.append([float(i) for i in string_tmp.split(',') if i !=''])
		return res

	def putEventClassVector(self, vectorList):
		for i, t in enumerate(vectorList):
			self.redis_conn.zadd("ClassVector",float(i+0.5),t)
		return None

	def putRecommendedList(self, uId, rList):
		result = self.mongo_conn.cUser.update({"_id": uId}, {"$set": {"recommend":rList, "count":len(rList), "re_time":int(time.time())}})
		if result != 1:
			return False
		else:
			return True

	def getEvents(self):
		eventsList = []
		events_cursor = self.mongo_conn.cEvent.find({}, {"cEvent_content":1, "vector":1})
		for event_cursor in events_cursor:
			if "vector" not in event_cursor:
				eventsList.append((event_cursor["_id"],event_cursor["cEvent_content"]))
		return eventsList

	def putEventFeature(self, eventId, eFeature):
		result = self.mongo_conn.cEvent.update({"_id":eventId},{"$set":{"vector":eFeature}})
		if result != 1:
			return False
		else:
			return True

			
if __name__ == '__main__':
	test = dbhelper()
	# users = test.getUserRecentRecord()
	# if users is not None:
	# 	print len(users)
	# 	for i in users:
	# 		print i











