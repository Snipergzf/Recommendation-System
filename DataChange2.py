#! /usr/bin/env python
#-*- conding:utf8 -*-
from dbhelper import dbhelper

class DataChange2(object):
	"""used to backup cUser data to 'cUser_bak' and clear the cUser collection of mongodb"""
	def __init__(self):
		self.dbhelper = dbhelper()
		self.mongoConn = self.dbhelper.mongo_conn

	def backup(self):
		usersList = self.mongoConn.cUser.find()
		for user in usersList:
			self.mongoConn.cUser_bak.insert_one(user)

	def clear(self):
		usersIdList = self.mongoConn.cUser.find({},{"_id":1})
		for userId in usersIdList:
			self.mongoConn.cUser.remove({"_id":userId["_id"]})
			self.mongoConn.cUser.insert_one({"_id":userId["_id"]})


if __name__ == '__main__':
	datachange2 = DataChange2()
	# datachange2.backup()
	datachange2.clear()
		
