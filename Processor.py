# -*- coding: UTF-8 -*-
import jieba
from gensim import corpora,models,similarities

#取数据
#预处理
class Processor(object):
	"""docstring for Processor"""
	def __init__(self,name):
		self.name=name
		stopWords_fn = './stop_words_cn.txt'
		self.stopWords_set = self.get_stopWords(stopWords_fn)

	def get_stopWords(self,stopWords_fn):
		
		stopWords_set=[]
		with open(stopWords_fn, 'rb') as f:
			for line in f:
				stopWords_set.append(line.strip('\r\n').decode('utf-8'))
			
		return stopWords_set

	def sentence2words(self,sentence):
		""" 
		split a sentence into words based on jieba
		"""

		
		

		return [word for word in jieba.cut(sentence) if word not in self.stopWords_set and word != ' ' and word.isspace()==False]

		

	def word2vec_model(self,name,num_features,sentences):
		
		min_word_count = 5
		num_workers = 48
		context = 20
		epoch = 20
		sample = 1e-5

		model = models.Word2Vec(
			sentences,
			size=num_features,
			min_count=min_word_count,
			workers=num_workers,
			sample=sample,
			window=context,
			iter=epoch,
		)
		model.save(name)

		return model




	def get_tfidf(self,docs):
		
		dictionary = corpora.Dictionary(docs)
		#dictionary.save('/tmp/docs.dict') 
		corpus = [dictionary.doc2bow(t) for t in docs]
		tfidf = models.TfidfModel(corpus=corpus)
		return tfidf,dictionary

	#此处的text_array为［(link，content)，...］形式
	def docs2tfidfs(self,exist_text_array,new_text_array):
		exist_docs=[]
		new_docs=[]
		for text in exist_text_array:
			tokens=self.sentence2words(text[1].strip(), True)
			exist_docs.append(tokens)
		for text in new_text_array:
			tokens=self.sentence2words(text[1].strip(), True)
			new_docs.append(tokens)
		tfidf_matrix,dictionary=self.get_tfidf(exist_docs+new_docs)
		exist_tfidf_array=[]
		new_tfidf_array=[]
		for k,doc in enumerate(exist_docs):
			exist_text_array.append(exist_text_array[k][0],tfidf_matrix[doc])

		for k,doc in enumerate(new_docs):
			new_tfidf_array.append(new_text_array[k][0],tfidf_matrix[doc])

		return exist_tfidf_array,new_tfidf_array
			
		

		