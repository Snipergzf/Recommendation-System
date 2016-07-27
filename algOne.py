#! /usr/bin/env python
#-*- coding:utf8 -*-

from config import *
import time
import math

class algOne(object):
	"""docstring for algOne"""
	def __init__(self, dbhelper, outputer, userList):
		self.dbhelper = dbhelper
		self.outputer = outputer
		self.userList = UserList

	def cal_Sim(self, vector1, vector2):
		a = sum([vector1[i]*vector2[i] for i in range(len(vector1))])
		b = math.sqrt(sum([i*i for i in vector1]))
		c = math.sqrt(sum([i*i for i in vector2]))
		return (a/(b*c))


	# there is something wrong!!!
	def match(self, uFeature, eventList):
		listF = [self.cal_Sim(uFeature, self.dbhelper.getEventVector(event)) for event in eventList]
		arg = sum(listF)/len(listF)
		list_to_return = []
		for index, item in enumerate(listF):
			if itme >= arg:
				dict_tmp = {"sim_value" = item, "id" = eventList[index]}
				list_to_return.append(dict_tmp)
		return list_to_return

	def alg(self):
		eventVectorList = self.dbhelper.getEventClassVector()
		if self.userList is None:
			return None
		for user in self.userList:
			re_feature, pa_feature = self.dbhelper.getUserFeture(user)
			if re_feature is None and pa_feature is None:
				pass
			else:
				delta = int(time.time())-re_feature["time"] 
				n = (delta*100)/86400
				#if re_feature is unexpired
				if math.abs(n-100) < 30:
					re_feature = re_feature["vector"]
					pa_feature = pa_feature["vector"]
				#if re_feature is expired
				else:
					re_feature = pa_feature["vector"]
					pa_feature = pa_feature["vector"]
				listH = [self.cal_Sim(pa_feature, classVector) for classVector in eventVectorList]
				arg = sum(listH)/len(listH)
				#select from listH where the value bigger than averge
				listG = [i for i, t in enumerate(listH) if t >= arg]
				listR = []
				for item in listG:
					eventList = self.dbhelper.getEventByClass(CATEGORY_LIST[item])
					#use match function to select recommendation event for user
					listF = self.match(re_feature, eventList)
					#put the recommended events in every class togather
					# listR = list(setR|set(listF))
					listR += listF
				self.outputer.matchList_output(uId, listR)





