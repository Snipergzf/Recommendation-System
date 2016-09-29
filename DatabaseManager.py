# -*- coding: UTF-8 -*-
import MySQLdb

class DatabaseManager:
	"""docstring for DatabaseManager"""
	def __init__(self, name):
		self.name = name

	def removeAll(self,databaseName,tableName):
		try:
			conn=MySQLdb.connect(host='127.0.0.1',user='webaccount',passwd='yujingwen',port=3306,charset='utf8')
			cur=conn.cursor()
			conn.select_db(databaseName)
			cur.execute('delete from %s.%s'%(databaseName,tableName))
			conn.commit()
			cur.close()
			conn.close()

			

		
		except MySQLdb.Error,e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])

		


	def getdataWithCondition(self,databaseName,tableName,infoWanted=None,condition=None):


		try:
			conn=MySQLdb.connect(host='127.0.0.1',user='webaccount',passwd='yujingwen',port=3306,charset='utf8')
			cur=conn.cursor()
			cur.execute('create database if not exists %s'%databaseName)
			conn.select_db(databaseName)
			
		   
			cur.execute('create table if not exists %s.%s(link varchar(200),title varchar(100),image varchar(200),source varchar(100),time varchar(100),content text,html text,place varchar(100),theme varchar(100),topic_distribution text,tag varchar(100))'%(databaseName,tableName))
			sql = 'select distinct '
			if infoWanted!=None:
				count=0
				for param in infoWanted:
					if count!=len(infoWanted)-1:
						sql=sql+param+","
					else:
						sql=sql+param
					count+=1
			else:
				sql=sql+"* "

			sql=sql+" from %s.%s "%(databaseName,tableName)

			if condition!=None:

				sql=sql+"where"

				count2=0
				for k in condition:
					if count2!=len(condition)-1:
						sql=sql+" %s='%s' and"%(k,condition.get(k))
					else:
						sql=sql+" %s='%s' "%(k,condition.get(k))
					count2+=1

			print "sql:%s"%sql 
			cur.execute(sql)
				
			results = cur.fetchall()
	  
			cur.close()
			conn.close()
			
			return results
		
		except MySQLdb.Error,e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])
			return []






	def muti_insert(self,databaseName,tableName,data_list):
		#print len(data_list[0])

		try:
			conn=MySQLdb.connect(host='127.0.0.1',user='webaccount',passwd='yujingwen',port=3306,charset='utf8')
			cur=conn.cursor()
			cur.execute('create database if not exists %s'%databaseName)
			conn.select_db(databaseName)
			cur.execute('create table if not exists %s.%s(link varchar(200),title varchar(100),image varchar(200),source varchar(100),time varchar(100),content text,html text,place varchar(100),theme varchar(100),topic_distribution text,tag varchar(100))'%(databaseName,tableName))
			sql = "insert into %s.%s(link, title, image,source,time,content,html,place,theme,topic_distribution,tag)"%(databaseName,tableName)+" values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)"
			cur.executemany(sql,data_list)
			conn.commit()
			cur.close()
			conn.close()

			

		
		except MySQLdb.Error,e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])

		
	  

  

	def store_event(self,databaseName,tableName,Event_dic):
		try:
			# conn=MySQLdb.connect(host='127.0.0.1',user='webaccount',passwd='yujingwen',port=3306,charset='utf8')
			# cur=conn.cursor()
			mysql_conn = MySQLdb.connect("127.0.0.1", "webaccount", "yujingwen", "Event")
			mysql_conn.set_character_set('utf8')
			cur = mysql_conn.cursor()
			cur.execute('SET NAMES utf8;')
			cur.execute('SET CHARACTER SET utf8;')
			cur.execute('SET character_set_connection=utf8;')
			
			#cur.execute('create database if not exists %s'%databaseName)
			#conn.select_db(databaseName)
			cur.execute('CREATE TABLE if not exists %s.%s(link varchar(200),title varchar(100),image varchar(200),source varchar(100),time varchar(100),content longtext,html text,place varchar(100),theme varchar(100),topic_distribution text,tag varchar(100))'%(databaseName,tableName))
			#print "insert into %s values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(tableName,Event_dic['link'],Event_dic['title'],Event_dic['image'],Event_dic['source'],Event_dic['time'],Event_dic['content'],Event_dic['html'],Event_dic['place'],Event_dic['theme'],Event_dic['tag'])
			cur.execute("insert into %s.%s values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(databaseName,tableName,
				Event_dic['link'],
				Event_dic['title'],
				Event_dic['image'],
				Event_dic['source'],
				Event_dic['time'],
				Event_dic['content'],
				Event_dic['html'],
				Event_dic['place'],
				Event_dic['theme'],
				"",
				Event_dic['tag']
				)
				)
			mysql_conn.commit()
			cur.close()
			mysql_conn.close()

		except MySQLdb.Error,e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])




	def updateDatabase(self,databaseName,tableName,data2update,condition=None):

		try:
			conn=MySQLdb.connect(host='127.0.0.1',user='webaccount',passwd='yujingwen',port=3306,charset='utf8')
			cur=conn.cursor()
			cur.execute('create database if not exists %s'%databaseName)
			conn.select_db(databaseName)
			
		   
			cur.execute('create table if not exists %s.%s(link varchar(200),title varchar(100),image varchar(200),source varchar(100),time varchar(100),content text,html text,place varchar(100),theme varchar(100),topic_distribution text,tag varchar(100))'%(databaseName,tableName))
			sql='update %s.%s set'%(databaseName,tableName)
			
			count1=0
			for k in data2update:
				if count1!=len(data2update)-1:
					sql=sql+" %s='%s' ,"%(k,data2update.get(k))
				else:
					sql=sql+" %s='%s' "%(k,data2update.get(k))
				count1+=1


			if condition!=None:
				sql=sql+"where"

				count2=0
				for k in condition:
					if count2!=len(condition)-1:
						sql=sql+" %s='%s' and"%(k,condition.get(k))
					else:
						sql=sql+" %s='%s' "%(k,condition.get(k))
					count2+=1




			
			print sql
			cur.execute(sql)
			conn.commit()
			cur.close()
			conn.close()
			
   
		
		except MySQLdb.Error,e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	  



	def removeEventWithUrl(self,databaseName,tableName,url):
		#print 'delete from %s.%s where link="%s"'%(databaseName,tableName,url)
	  
		try:
			conn=MySQLdb.connect(host='127.0.0.1',user='webaccount',passwd='yujingwen',port=3306,charset='utf8')
			cur=conn.cursor()
			cur.execute('create database if not exists %s'%databaseName)
			conn.select_db(databaseName)
		  
			cur.execute('create table if not exists %s.%s(link varchar(200),title varchar(100),image varchar(200),source varchar(100),time varchar(100),content text,html text,place varchar(100),theme varchar(100),topic_distribution text,tag varchar(100))'%(databaseName,tableName))
			cur.execute('delete from %s.%s where link="%s"'%(databaseName,tableName,url))
			conn.commit()
			cur.close()
			conn.close()
			

		except MySQLdb.Error,e:
			print "Mysql Error %d: %s" % (e.args[0], e.args[1])
				

		def removeOldDataWithDate(now_month,now_day):
			#先取豆瓣数据出来处理为年月日当前时间对比
			data_set=[]
			try:
				conn=MySQLdb.connect(host='127.0.0.1',user='webaccount',passwd='yujingwen',port=3306,charset='utf8')
				cur=conn.cursor()
				cur.execute('create database if not exists %s'%databaseName)
				conn.select_db(databaseName)
			
				for tableName in tableName_array:
					cur.execute('create table if not exists %s.%s(link varchar(200),title varchar(100),image varchar(200),source varchar(100),time varchar(100),content text,html text,place varchar(100),theme varchar(100),topic_distribution text,tag varchar(100))'%(databaseName,tableName))
					cur.execute('select distinct link,time from %s'%tableName)
					results = cur.fetchall()
					for row in results:
						data_set.append((row[0],row[1]))
				cur.close()
				conn.close()
			
	   
		
			except MySQLdb.Error,e:
				print "Mysql Error %d: %s" % (e.args[0], e.args[1])
				
			for item in data_set:
				event_end_date=""
				comps=item[1].split(" ")
				if len(comps[0])==len(comps[1]):
					event_end_date=comps[1]
				else:
					event_end_date=comps[0]

				month=int(event_end_date.split("月")[0])
				day=int(event_end_date.split("月")[1].split("日")[0])
				if now_month>month and now_day>day:
					#删除数据
					self.deleteEventWithUrl(item[0])












