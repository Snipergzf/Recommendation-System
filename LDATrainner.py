#-*- coding:utf-8 -*-
from Processor import Processor
from DatabaseManager import DatabaseManager
from gensim import corpora,models
import os

class LDATrainner():
	"""docstring for LDATrainner"""
	def __init__(self, name,dimension):
		#这里LDA的建立需要综合来自各个来源的文件
		self.name=name
		self.dimension=dimension
		self.processor=Processor("LDA_processor")
		self.databaseManager=DatabaseManager("LDA_dbHelper")
		if os.path.exists("ldaModel%d"%self.dimension):
			self.ldaModel=models.LdaModel.load("ldaModel%d"%self.dimension, mmap='r')
		else:
			self.ldaModel=None

		if os.path.exists("Lda_dictionary"):
			self.dic=corpora.Dictionary.load_from_text("Lda_dictionary")
		else:
			self.dic=None

	def train(self):
		#train_set=self.databaseManager.getdataWithCondition("Event","douban_train",infoWanted=["theme" , "content"])+self.databaseManager.getdataWithCondition("Event","haitou_train",infoWanted=["theme" , "content"])+self.databaseManager.getdataWithCondition("Event","expired_train",infoWanted=["theme" , "content"])
		train_set=self.databaseManager.getdataWithCondition("Event","expired_train",infoWanted=["theme" , "content"])
		docs_bow_list=[]
		for item in train_set[0:100]:
			docs_bow_list.append(self.processor.sentence2words(item[1].strip()))
			print "%d"%len(docs_bow_list)
		#docs_bow_list=[self.processor.sentence2words(item[1].strip()) for item in train_set]
			


	
		
		self.dic = corpora.Dictionary(docs_bow_list)
		self.dic.save_as_text("Lda_dictionary")

		corpus = [self.dic.doc2bow(terms_of_doc) for terms_of_doc in docs_bow_list]
		print "corpus"

		self.ldaModel = models.LdaModel(corpus, num_topics = self.dimension,id2word=self.dic)
		self.ldaModel.save("ldaModel%d"%self.dimension)


	def getLdaDistribution(self,event_content):
		lda_distribution=self.ldaModel[self.dic.doc2bow(self.processor.sentence2words(event_content))]
		#转换成一个向量
		lda_vec=[item[1] for item in lda_distribution]
		return lda_vec






	   
