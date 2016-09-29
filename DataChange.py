#! /usr/bin/env python
#-*- conding: utf8 -*-

from dbhelper import dbhelper
import Queue
import time
import threading


class DataChange(object):
	"""This program is used to change the data structure"""
	def __init__(self):
		self.dbhelper = dbhelper()
		self.mongoConn = self.dbhelper.mongo_conn
		self.queue_share = Queue.Queue(maxsize = 20)
		self.null_count = 0
		self.doc_count = 0

	def producer(self):
		usersList = self.mongoConn.cUser.find()
		for i in usersList:
			self.queue_share.put(i, block=True)


	def consumer(self):
		while not self.queue_share.empty():
			userData = self.queue_share.get()
			doc_insert = {"_id":userData["_id"]}
			pList = sList = cList= None
			if "participated_event" in userData:
				pList = userData["participated_event"]
			if "shared_event" in userData:
				sList = userData["shared_event"]
			if "clicked_event" in userData:
				cList = userData["clicked_event"]
			while 1:
				if pList is None and sList is None and cList is None:
					# self.mongoConn.cUser_test.insert_one(doc_insert)
					pass
					print("insert null doc: "+userData["_id"])
					self.null_count += 1
					break
				
				eventsList = []
				if pList is not None and sList is not None:
					for i in pList:
						if i in sList:
							try:
								action_time = int(self.mongoConn.cEvent.find_one({"_id":i},{"cEvent_publish":1})["cEvent_publish"])
							except Exception, e:
								action_time = int(time.time())
							sList.remove(i)
							try:
								cList.remove(i)
							except Exception, e:
								print(i + " is not in " + userData["_id"] + "'s cList")
							tmp = {"event_id":i,"time":action_time,"action":["participate","share","click"]}
						else:
							try:
								action_time = int(self.mongoConn.cEvent.find_one({"_id":i},{"cEvent_publish":1})["cEvent_publish"])
							except Exception, e:
								action_time = int(time.time())
							try:
								cList.remove(i)
							except Exception, e:
								print(i + " is not in " + userData["_id"] + "'s cList")
							tmp = {"event_id":i,"time":action_time,"action":["participate","click"]}
						eventsList.append(tmp)
					if sList is not None:
						for j in sList:
							try:
								action_time = int(self.mongoConn.cEvent.find_one({"_id":j},{"cEvent_publish":1})["cEvent_publish"])
							except Exception, e:
								action_time = int(time.time())
							try:
								cList.remove(j)
							except Exception, e:
								print(j + " is not in " + userData["_id"] + "'s cList") 
							tmp_ = {"event_id":j,"time":action_time,"action":["share","click"]}
							eventsList.append(tmp_)
				elif pList is None and sList is not None:
					for j in sList:
						try:
							action_time = int(self.mongoConn.cEvent.find_one({"_id":j},{"cEvent_publish":1})["cEvent_publish"])
						except Exception, e:
							action_time = int(time.time())
						try:
							cList.remove(j)
						except Exception, e:
							print(j + " is not in " + userData["_id"] + "'s cList")
						tmp_ = {"event_id":j,"time":action_time,"action":["share","click"]}
						eventsList.append(tmp_)
				elif pList is not None and sList is None:
					for j in pList:
						try:
							action_time = int(self.mongoConn.cEvent.find_one({"_id":j},{"cEvent_publish":1})["cEvent_publish"])
						except Exception, e:
							action_time = int(time.time())
						try:
							cList.remove(j)
						except Exception, e:
							print(j + " is not in " + userData["_id"] + "'s cList")
						tmp_ = {"event_id":j,"time":action_time,"action":["participate","click"]}
						eventsList.append(tmp_)
				elif cList is not None:
					for i in cList:
						try:
							action_time = int(self.mongoConn.cEvent.find_one({"_id":i},{"cEvent_publish":1})["cEvent_publish"])
						except Exception, e:
							action_time = int(time.time())
						cList.remove(i)
						tmp_ = {"event_id":i,"time":action_time,"action":["click"]}
						eventsList.append(tmp_)
				doc_insert["events"] = eventsList
				self.mongoConn.cUser_test.insert_one(doc_insert)
				print("insert doc: "+userData["_id"])
				self.doc_count += 1
				break

	def main(self):
		while True:
			try:
				print("start...")
				time_count = time.time()
				threads = []
				consumerThread1= threading.Thread(target=self.consumer)
				consumerThread2= threading.Thread(target=self.consumer)
				producerThread = threading.Thread(target=self.producer)
				producerThread.start()
				time.sleep(2)
				consumerThread1.start()
				consumerThread2.start()
				threads.append(consumerThread1)
				threads.append(consumerThread2)
				threads.append(producerThread)
				for thread in threads:
					thread.join()
				delta = time.time()-time_count
				print ("there are %d null doc"%(self.null_count))
				print ("there are %d docs have content"%(self.doc_count))
				print ("it costs %f seconds to run"%(delta))
				print("exit.")
				break
			except KeyboardInterrupt:
				break

if __name__ == '__main__':
	datachange = DataChange()
	datachange.main()





















