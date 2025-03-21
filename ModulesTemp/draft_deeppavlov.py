
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

#TESTING
#from deeppavlov import build_model, configs
#ner_model = build_model(configs.ner.ner_rus_bert, download=False)

#sentences = [
#                [''],
#                #['Они скажут что ты дилетантка'],
#                #['Да.'],
#                #['Они сейчас то смотрят на тебя, типо "ну ты ниче не понимаешь"))))'],
#                #['Буду травить, как мы тусили'],
#                #['И я о том же))))'],
#                #['Бабуличка скажут юличка идите семечки на лавочке жамкайте, что вы нам тут анекдоты рассказываете)))'],
#                #['Стопудов'],
#                #['Надо на какой нибудь не интересный фильм'],
#                ['Я расскажу внукам, когда у них будет туса, лет в 25,я буду клевой бабкой'],
#                [''],
#                [')'],
#                ['']
#            ]

#sentences = [ '', '', 'fevv','  \n ', ')', '']

#for i in range(len(sentences)-1,-1,-1):
#    if sentences[i] =='' or sentences[i].isspace():
#        sentences.pop(i)


#for sent in sentences:
#    try:
#        res = ner_model(sentences)
#        print('Ok: '+str(sent))
#        print('     '+str(res))
#    except:
#        print('Error: '+str(sent))

#import sys
#sys.exit(0)

#### Named Entity Recognition

from deeppavlov import configs, build_model
#from deeppavlov.models.morpho_tagger.lemmatizer import UDPymorphyLemmatizer
#import tensorflow_core.python.pywrap_tensorflow_internal

ner_model = build_model(configs.ner.ner_rus_bert, download=False)

#res = ner_model(['Bob Ross lived in Florida'])
#res = ner_model(['Иван Петров живет в Российской Федерации в Пензе', 'Давайте встремся у Кинотеатра Современник', 'Иван Петров живет в Российской Пензе','Путешествие в Южную Африку','Путешествие в Африку'])
res = ner_model(['Адрес: Челябинск, пр-кт Ленина, 21-А, 1 этаж отеля Меридиан.'])

tokens = res[0]
ners = res[1]

print('TOKENS:')
print([f'{i}\n' for i in res[0]])

import sys
sys.exit(0)


Lemmatizer = UDPymorphyLemmatizer()
for sent in res[0]:
    for word in sent:
        print(str(Lemmatizer(word)))
    pass


print('  ') 
print('NERS:')
print([f'{i}\n' for i in res[1]])


##########################################################################################
#### Sentence Tokenizer

import deeppavlov.models.tokenizers.ru_sent_tokenizer as ru_sent_tokenizer
from deeppavlov.models.tokenizers.ru_sent_tokenizer import RuSentTokenizer
from deeppavlov.models.tokenizers.ru_tokenizer import RussianTokenizer  #words only
from deeppavlov.models.tokenizers.nltk_tokenizer import NLTKTokenizer
from deeppavlov.models.tokenizers.nltk_moses_tokenizer import NLTKMosesTokenizer
from nltk.tokenize import sent_tokenize

#ru_sent_tokenizer.SHORTENINGS.add('пр')
ru_sent_tokenizer.JOINING_SHORTENINGS.add('пр')

RST = RuSentTokenizer()
#RST = RussianTokenizer()
#RST = NLTKTokenizer(tokenizer = 'sent_tokenize')
#RST = NLTKMosesTokenizer()

#res = RST(['Эта шоколадка за 400р. ничего из себя не представляла. Артём решил больше не ходить в этот магазин'])
#res = RST(['Работа в Челябинске (Пр. Ленина 81)'])
res = RST([ 'Адрес: Челябинск, пр-кт Ленина, 21-А, 1 этаж отеля Меридиан.'])


#RST = sent_tokenize
#res = RST('Работа в Челябинске (Пр. Ленина 81)', language='russian')


print(res)

import sys
sys.exit(0)

#### Lemmatize

from deeppavlov import build_model, configs
#model = build_model(configs.morpho_tagger.BERT.morpho_ru_syntagrus_bert, download=True)
ner_model = build_model(configs.ner.ner_rus_bert, download=False)
model = build_model(configs.morpho_tagger.UD2_0.morpho_ru_syntagrus_pymorphy_lemmatize, download=True)

#sentences = ["Я шёл домой по незнакомой улице.", "Девушка пела в церковном хоре о всех уставших в чужом краю."]
#sentences = ['Иван Петров живет в Российской Федерации в Пензе', 'Давайте встремся у Кинотеатра Современник', 'Иван Петров живет в Российской Пензе','Путешествие в Южную Африку','Путешествие в Африку']

#sentences = ['Перед вами Пабло Эмилио Эскобара', 'Перед вами Эскобара Пабло Эмилио', 'Перед вами Эмилио Эскобара Пабло', 'Перед вами Пабло Эмилио Эскобара и Блин Клинтон', 'Перед вами Пабло Эмилио Эскобара и Клинтон Билл']
#sentences = ['К нам приехали Иван Петров, Пабло Эмилио Эскобара, Блин Клинтон, Клинтон Билл и кто-то еще', 'К нам приехали пабло эмилио иван петров, Эскобара, блин Клинтон, Клинтон Билл и кто-то еще']
sentences = ['Иван Петр Сергей Эмиль Ким Чен Ир', 'К нам приехали пабло эмилио иван петров, Эскобара, блин Клинтон, Клинтон Билл и кто-то еще']

res = ner_model(sentences)

for parse in model(sentences):
    print(parse)

tokens = res[0]
ners = res[1]

print('  ') 
print('NERS:')
print([f'{i}\n' for i in res[1]])
print('  ') 
print('  ') 
print('  ') 

for res_el in zip(tokens, ners):
    print(f'{res_el[0]}')
    print(f'{res_el[1]}')
    print('  ') 

import sys
sys.exit(0)

##########################################################################################
#### SENTIMENT


from deeppavlov import configs, build_model

#ner_model = build_model(configs.classifiers.rusentiment_elmo_twitter_cnn, download=False)  
###tree['classifiers']['rusentiment_elmo_twitter_cnn']
##res = ner_model(['Bob Ross lived in Florida'])


#import deeppavlov.settings
#from pathlib import Path
#model = Path.cwd() / 'Config' / 'ner_rus_bert.json'
##model = configs.classifiers.rusentiment_elmo_twitter_cnn
#ner_model = build_model(model, download=True)  

#sentenses = ['Иван Петров живет в Пензе', 'Семен Семенович хороший человек',
#                 'Вася много курит',
#                 'Он хороший человек, но выпил плохой кофе',
#                 'Он хороший человек, но выпил плохой, точнее отвратительный, кофе',
#                 'Он хороший человек, но выпил плохой, точнее отвратительный, кофе и неожиданно выздоровел',
#                 'Я вышел из лесу, был сильный мороз',
#                 'Но были люди в наше время, не то что нынешнее племя'
#                 ]

#res = ner_model(sentenses)

#print(res)

