#! /usr/bin/env python
#-*- conding: utf8 -*-

from dbhelper import dbhelper
import threading


class datainsert(object):
	"""This program is used to insert the data into database"""
	def __init__(self):
		self.dbhelper = dbhelper()
		self.mongoConn = self.dbhelper.mongo_conn

	def userInsert(self):
		doc1 = {
				"_id":"166",
				"events":[{
						"action":["click"],
						"event_id":"1458525349",
						"time":1469609300
					}]
			}
		doc2 = {
				"_id":"167",
				"events":[{
						"action":["click"],
						"event_id":"1458525350",
						"time":1469609300
					}]
			}
		self.mongoConn.cUser_test.insert_one(doc1)
		self.mongoConn.cUser_test.insert_one(doc2)
	
	def eventInsert(self):
		doc1 = {
				"_id":"1458525349",
				"vector":[1,0,0,0,0],
				"cEvent_theme":"娱乐",
				"cEvent_figure":"", "cEvent_tag":"", "cEvent_name":"","click_num":"","cEvent_publish":"1469606790"
			
			}
		doc2 = {
				"_id":"1458525350",
				"vector":[0,1,0,0,0],
				"cEvent_theme":"讲座",
				"cEvent_figure":"", "cEvent_tag":"", "cEvent_name":"","click_num":"","cEvent_publish":"1469606790"
			}
		doc3 = {
				"_id":"1458525351",
				"vector":[0,0,1,0,0],
				"cEvent_theme":"招聘",
				"cEvent_figure":"", "cEvent_tag":"", "cEvent_name":"","click_num":"","cEvent_publish":"1469606790"
			}
		doc4 = {
				"_id":"1458525352",
				"vector":[0,0,0,1,0],
				"cEvent_theme":"其他",
				"cEvent_figure":"", "cEvent_tag":"", "cEvent_name":"","click_num":"","cEvent_publish":"1469606790"
			}
		self.mongoConn.cEvent_test.insert_one(doc1)
		self.mongoConn.cEvent_test.insert_one(doc2)
		self.mongoConn.cEvent_test.insert_one(doc3)
		self.mongoConn.cEvent_test.insert_one(doc4)

	def main(self):
		self.userInsert()
		self.eventInsert()

if __name__ == '__main__':
	insert = datainsert()
	insert.main()





















