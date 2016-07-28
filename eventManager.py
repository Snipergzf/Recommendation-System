#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from config import *

class eventManager(object):
	"""docstring for eventManager"""
	def __init__(self, dbhelper, outputer):
		self.dbhelper = dbhelper
		self.outputer = outputer

	# def getNewEventVector(self, eventId):
	# 	#get str type event
	# 	event = self.redisConn.get(eventId)
	# 	#convert str to dict type
	# 	event2dict = json.loads(event)
	# 	#return the 'vector' list
	# 	return (event2dict['vector'])
	def vectorOverlay(self, seq1, seq2):
		return [seq1[i]+seq2[i] for i in range(len(seq1))]

	def cal_vector(self,oneEvent):
		self.dbhelper.getEventVector(oneEvent)
		
	def cal_classFeature(self):
		classVectorList = []
		for i in CATEGORY_LIST:
			#get the list of the very class of events
			eventList = self.dbhelper.getEventByClass(i)
			n = len(eventList)
			if n==1:
				classVector = self.dbhelper.getEventVector(eventList[0])
			elif n>1:
				vectorlist = map(self.cal_vector, eventList)
				classVector = reduce(self.vectorOverlay(), vectorlist)
				classVector = [x/n for x in classVector]
			classVectorList.append(classVector)
		self.outputer.class_vector_output(classVectorList)