
#########################################################
#загрузка словаря
#python -m deeppavlov download ner_ontonotes_bert_mult

#установка предобученной модели ner_rus_bert 
#в командной строке: interact ner_rus_bert -d
#установка предобученной модели rusentiment_elmo_twitter_cnn 
#в командной строке:interact rusentiment_elmo_twitter_cnn -d

#установка bert_dp
#pip install -r  C:\Work\Python\CasCrawl37\CassandraPy37\Lib\site-packages\deeppavlov\requirements\bert_dp.txt

#import deeppavlov.deep as deep

#deep.main()    
#########################################################




#from deeppavlov import configs, build_model

#ner_model = build_model(configs.ner.ner_rus_bert, download=False)

##res = ner_model(['Bob Ross lived in Florida'])
#res = ner_model(['Иван Петров живет в Пензе'])

#print(res)


from deeppavlov import configs, build_model

ner_model = build_model(configs.ner.rusentiment_elmo_twitter_cnn, download=True)

#res = ner_model(['Bob Ross lived in Florida'])
res = ner_model(['Иван Петров живет в Пензе'])

print(res)