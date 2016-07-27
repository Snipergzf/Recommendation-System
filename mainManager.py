#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Copyright 2016 gzf

from userManager import userManager
from eventManager import eventManager
from outputer import outputer
from algOne import algOne
from producer import producer
from config import *
from dbhelper import dbhelper
import threading
import time
# from gevent import monkey; monkey.patch_all()
# import gevent
import random


class mainManager(object):
	"""docstring for mainManager"""
	def __init__(self):
		self.dbhelper = dbhelper()
		self.outputer = outputer(self.dbhelper)
		self.eManager = eventManager(self.dbhelper, self.outputer)
		self.uManager = userManager(self.dbhelper, self.outputer)
		self.pro = producer(self.dbhelper.mongo_conn, self.dbhelper.redis_conn)
		self.algone = algOne(self.dbhelper, self.outputer)
		self.userList = []
		self.lock1 = threading.Lock()
		self.lock2 = threading.Lock()

	def eventWorker(self):
		while True:
			if self.lock1.acquire()
				self.pro.main()
				self.eManager.cal_classFeature()
				self.lock1.release()
				time.sleep(INTERVAL+random.randint(-100,100))

	def userWorker(self):
		while True:
			if self.lock2.acquire()
				self.userList = self.uManager.cal_userFeature()
				self.lock2.release()
				time.sleep(INTERVAL+random.randint(-100,100))

	def algWorker(self):
		while True:
			if self.lock1.acquire() and self.lock2.acquire():
				self.algone.alg(self.userList)
				self.lock1.release()
				self.lock2.release()
				time.sleep(INTERVAL+random.randint(-100,100))
			time.sleep(INTERVAL_ALG)


	def main(self):
		try:
			print('Initializing...')
			threads = [
				eventThread = threading.Thread(target=eventWorker, args=()),
				userThread = threading.Thread(target=userWorker, args=()),
				algThread = threading.Thread(target=algWorker, args=()),
			]
			for thread in threads:
				thread.start()
			for thread in threads:
				thread.join()
		except KeyboardInterrupt:
			print ('Friendly exits.')
			return None



if __name__ == '__main__':
	mainManager = mainManager()
	mainManager.main()








