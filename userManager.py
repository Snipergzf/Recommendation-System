#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
import time
from config import *

class userManager(object):
"""docstring for userManager"""
	def __init__(self, dbhelper, outputer):
		self.dbhelper = dbhelper
		self.outputer = outputer

	def decay(self, k):
		res = math.exp(-k*DECAY);
		return res

	def cal_vector(self,oneRecord):
		event_vector = self.dbhelper.getEventVector(i['e_id'])
			if "participate" in i['action']:
				return [PARTICIPATE_WEOGHT*i for i in event_vector]
			elif "share" in i['action']:
				return [SHARE_WEIGHT*i for i in event_vector]
			else:
				return [CLICK_WEIGHT*i for i in event_vector]

	def vectorOverlay(self, seq1, seq2):
		return [seq1[i]+seq2[i] for i in range(len(seq1))] 

	def cal_recentFeature(self, record):
		vectorlist = map(self.cal_vector, record)
		vector = reduce(self.vectorOverlay,vectorlist)
		norm  = sum([i*i for i in vector])
		return [x/norm for x in vector]

	def cal_pastFeature(self, pastFeature):
		detal = pastFeature['time'] - int(time.time())
		pa_feature = pastFeature['vector']
		n = (detal*100)/(86400*3)
		if math.abs(n-100)>10:
			l = self.decay(detal/(86400.0))
			pa_feature_tmp = [l*i for i in pa_feature['vector']]
			pa_feature = self.vectorOverlay(pa_feature_tmp,re_feature)
		return pa_feature

	def cal_userFeature(self):
		# get uid list of recent online users
		users = self.dbhelper.getRecentUser()
		if users is not None:
			#calculate feature vector for every user
			for user in users:
				#get record of every user which is a dict of eventId,time,action
				recentRecord, pastFeature = self.dbhelper.getUserRecord(user)
				if recentRecord is not None:
					#if user has record, calculate his(her) vector
					re_feature = self.cal_recentFeature(recentRecord)
				else:
					#else the user is the fresh man or long time offline, return None
					re_feature = None
				if pastFeature is not None:
					pa_feature = self.cal_pastFeature(pastFeature)
				else:
					pa_feature = None
				try:
					self.outputer.user_feature_output(user, re_feature, pa_featureve)
				except Exception, e:
					pass
		return users



