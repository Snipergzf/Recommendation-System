#!/usr/bin/env python
# -*- coding: utf8 -*-
#Copyright 2016 gzf

class outputer(object):
	"""docstring for output"""
	def __init__(self, dbhelper):
		self.dbhelper = dbhelper

	def user_feature_output(self, uId, re_feature, pa_feature):
		if self.dbhelper.putUserFeature(uId, re_feature, pa_feature):
			return True
		else:
			return False

	def class_vector_output(self, vectorList):
		self.dbhelper.putEventClassVector(vectorList)
		return None

	def matchList_output(self, uId, recommendList):
		self.dbhelper.putRecommendedList(uId, recommendList)
		return None

	def output(self, user, data, flag=False):
		if flag:
			user_data = self.mongoConn.cUser.find_one({"_id":user})
			data_pre = user_data['recom']
			data = data + data_pre
			events = self.redisConn.zrange("all", 0, -1)
			data_update = [x for x in data if x in events]
			user_data['recom'] = data_update
			self.mongoConn.cUser.update({"_id":user},user_data)
		else:
			user_data = self.mongoConn.cUser.find_one({"_id":user})
			user_data["recom"] = data
			self.mongoConn.cUser.update({"_id":user},user_data)


	# """reverse the event-user matrix to user_event matrix"""
	# def reverse(self):
	# 	pass
