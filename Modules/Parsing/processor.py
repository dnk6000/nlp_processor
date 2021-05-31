import Modules.Common.const as const
import Modules.Common.common as common
import Modules.Common.globvars as globvars

import Modules.Parsing.ner as ner
import Modules.Parsing.lemma as lemma
import Modules.Parsing.token as token
import Modules.Parsing.sentiment as sentiment

import Modules.Crawling.exceptions as exceptions

class DataProcessor(common.CommonFunc):
    def __init__(self, *args,
                 db,
                 id_project,
                 id_www_source,
                 need_stop_cheker = None,
                 portion_size = 50,
                 **kwargs):
        
        super().__init__(*args, **kwargs)

        self.db = db
        self.need_stop_checker = need_stop_cheker
        self.id_project = id_project
        self.id_www_source = id_www_source
        self.portion_size = portion_size


    def process(self):
        try:
            self._process()
        except exceptions.UserInterruptByDB as e:
            self.log_critical_error(e)
        except Exception as e:
            self.log_critical_error(e)
        pass

    def _process(self):
        pass

    def check_user_interrupt(self):
        if self.need_stop_checker is None:
            return False
        self.need_stop_checker.need_stop()

    def log_critical_error(self, raised_exeption):
        pass

class SentimentProcessor(DataProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.raw_text = {}

        self.sent_tokenizer = token.SentenceTokenizer()
        self.sentim_analizer = sentiment.SentimentAnalizer()

        self.debug_num_portions = 3 #number portions to stop process

    def get_text_portion(self):
        self.raw_text = self.db.data_text_select_unprocess(self.id_www_source, self.id_project, number_records = self.portion_size)

    def _process(self):
        portion_counter = 1
        self.db.log_trace('Get portion for SentimentProcessor', self.id_project, 'Portion: {}'.format(portion_counter))
        self.get_text_portion()

        while len(self.raw_text) > 0:
            self.check_user_interrupt()

            #tokenize text to the sentences
            text_only = [i['content'] for i in self.raw_text]
            sentence_tokens = self.sent_tokenizer.tokenize(text_only)

            #sentiment analysis of the text as a whole
            self.debug_msg('---sentim text analize. portion = {}'.format(portion_counter))
            sentiment_text = self.sentim_analizer.analize(text_only)

            #collect sentences in a solid list - to speed up tonalization
            c = 0
            solid_sentence_list = []
            owner_sentence_list = []
            for sentences in sentence_tokens:
                if len(sentences) == 0:
                    solid_sentence_list.append('')
                    owner_sentence_list.append(c)
                else:
                    for sent in sentences:
                        solid_sentence_list.append(sent)
                        owner_sentence_list.append(c)
                c += 1

            #sentiment analysis of the sentences
            solid_sentiment_sent = self.sentim_analizer.analize(solid_sentence_list) 

            #convert solid list to original
            prev_index = -1
            _sent = []
            sentiment_sent = []
            for res in zip(solid_sentiment_sent,owner_sentence_list):
                sent  = res[0]
                index = res[1]
                if index != prev_index and prev_index != -1:
                    sentiment_sent.append(_sent)
                    _sent = []
                _sent.append(sent)
                prev_index = index
            sentiment_sent.append(_sent)

            #saving results
            for res in zip(self.raw_text, sentiment_text, sentence_tokens, sentiment_sent):
                res_raw_text        = res[0]
                res_sentiment_text  = res[1]
                res_sentence_tokens = res[2]
                res_sentiment_sent  = res[3]

                id_data_text = res_raw_text['id']
                rating_text = res_sentiment_text

                self.debug_msg('Upsert sentence token: id data text = {}  SENT: {}'.format(id_data_text, res_sentence_tokens))
                sent_token_id = self.save_token_result(id_data_text, res_sentence_tokens)

                self.debug_msg('Upsert sentiment text: id data text = {}  RATE: {}'.format(id_data_text, rating_text))
                self.save_sentim_text(id_data_text, self.get_rating_id(rating_text))

                for sent_token in zip(res_sentence_tokens, res_sentiment_sent, sent_token_id):
                    rating_sent = sent_token[1]
                    id_token_sentence = sent_token[2]['upsert_sentence']
                    self.debug_msg('Upsert sentiment sentence: id data text = {}  RATE: {} ID SENT TOKEN: {}'.format(id_data_text, rating_sent, id_token_sentence))
                    self.save_sentim_sentence(id_data_text, id_token_sentence, self.get_rating_id(rating_sent))

                self.db.data_text_set_is_process(id_data_text, autocommit = False)

                self.db.commit()

            if self.debug_mode and portion_counter >= self.debug_num_portions:
                break

            portion_counter += 1
            self.db.log_trace('Get portion for SentimentProcessor', self.id_project, 'Portion: {}'.format(portion_counter))
            self.get_text_portion()
        pass

    def get_rating_id(self, rate_txt):
        if rate_txt in const.SENTIM_RATE:
            return const.SENTIM_RATE[rate_txt]
        else:
            return const.SENTIM_RATE['error']

    def save_token_result(self, id_data_text, sentence_tokens):
        return self.db.token_upsert_sentence(self.id_www_source, self.id_project, id_data_text, sentence_tokens, autocommit = False)

    def save_sentim_text(self, id_data_text, id_rating):
        self.db.sentiment_upsert_text(self.id_www_source, self.id_project, id_data_text, id_rating, autocommit = False) 

    def save_sentim_sentence(self, id_data_text, id_token_sentence, id_rating):
        self.db.sentiment_upsert_sentence(self.id_www_source, self.id_project, id_data_text, id_token_sentence, id_rating, autocommit = False) 

    def log_critical_error(self, raised_exeption):
        err_description = exceptions.get_err_description(raised_exeption, raw_text = str(self.raw_text))

        self.db.log_fatal(str(raised_exeption), self.id_project, err_description)

class NerProcessor(DataProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.raw_sentences = []

        self.ner_recognizer = ner.NerRecognizer()
        self.lemmatizer = lemma.Lemmatizer()

        self.debug_num_portions = 3 #number portions to stop process

        self.NER_ENT_TYPES = globvars.GlobVars().get('NER_ENT_TYPES')

        self.debug_raw_sentences = []
        pass

    def get_raw_sentences(self):
        self.raw_sentences = self.db.sentence_select_unprocess(self.id_www_source, self.id_project, number_records = self.portion_size)
        if self.debug_mode and len(self.debug_raw_sentences) > 0:
            for i in range(0,len(self.debug_raw_sentences)-1):
                if i > len(self.raw_sentences):
                    break
                self.raw_sentences[i]['txt'] = self.debug_raw_sentences[i]



    def _process(self):
        portion_counter = 0

        while True:
            self.check_user_interrupt()

            portion_counter += 1
            self.debug_msg('Get portion for NerProcessor Portion: {}'.format(portion_counter))
            self.db.log_trace('Get portion for NerProcessor', self.id_project, 'Portion: {}'.format(portion_counter))
            self.get_raw_sentences()
            self.remove_empty_sentencess()

            _sentences = [i['txt'] for i in self.raw_sentences]

            ner_result = self.ner_recognizer.recognize(_sentences)
            lemma_result = self.lemmatizer.lemmatize(_sentences)

            for result in zip(self.raw_sentences, ner_result[0], ner_result[1], lemma_result):
                #record that the sentence has been processed
                id_sentence = result[0]['id']
                self.save_set_is_process(id_sentence)

                #record word tokens
                id_data_text = result[0]['id_data_text']
                words_array = result[1]                          #get words tokens from ner processor
                lemms_array = [_['lemma'] for _ in result[3]]
                if len(words_array) != len(lemms_array):
                    _lemma_result = self.lemmatizer.lemmatize([' '.join([_ for _ in words_array])])
                    lemms_array = [_['lemma'] for _ in _lemma_result[0]]

                    if len(words_array) != len(lemms_array):
                        self.log_error_ner_vs_lemma_mismatch(id_data_text, id_sentence, words_array, lemms_array)
                        continue
                        #words_array = [_['word'] for _ in result[3]] #get words tokens from lemmatizer

                word_id_list = self.db.token_upsert_word(self.id_www_source, self.id_project, id_data_text, id_sentence, words_array, lemms_array, autocommit = False)

                #record named entities
                ners = result[2]
                ners_id = self.convert_ners_to_id(ners)

                for wrd in zip(word_id_list, ners_id, lemms_array):
                    id_ent_type = wrd[1]
                    if id_ent_type is None:
                        continue
                    id_word = wrd[0]['upsert_word']
                    txt_lemm = wrd[2]
                    self.db.entity_upsert(self.id_www_source, self.id_project, id_data_text, id_sentence, id_word, id_ent_type, txt_lemm, autocommit = False)


            self.db.commit()

            if self.debug_mode and portion_counter >= self.debug_num_portions:
                break

        pass


    def remove_empty_sentencess(self):
        for i in range(len(self.raw_sentences)-1,-1,-1):
            if self.raw_sentences[i]['txt'] == '' or self.raw_sentences[i]['txt'].isspace():
                self.save_set_is_process(self.raw_sentences[i]['id'])
                self.raw_sentences.pop(i)

    def save_set_is_process(self,id_sentence):
        self.db.sentence_set_is_process(id = id_sentence, autocommit = False)


    def convert_ners_to_id(self, ner_list):
        _ner_id_list = []
        for ner_code in ner_list:
            if not ner_code in self.NER_ENT_TYPES:
                new_ent_type_id = self.db.ent_type_insert(ner_code, 'its new entity', autocommit = False)
                self.NER_ENT_TYPES[ner_code] = { 'id': new_ent_type_id['id'], 'not_entity': False }
            if self.NER_ENT_TYPES[ner_code]['not_entity']:
                _ner_id_list.append(None) 
            else:
                _ner_id_list.append(self.NER_ENT_TYPES[ner_code]['id']) 
        
        return _ner_id_list

    def log_critical_error(self, raised_exeption):
        err_description = exceptions.get_err_description(raised_exeption, raw_sentences = str(self.raw_sentences))

        debug_array_id = 'debug_id_sent_list = ['+ ','.join([str(i['id']) for i in self.raw_sentences]) + ']'
        err_description = err_description + '\n' + debug_array_id

        self.db.log_fatal(str(raised_exeption), self.id_project, err_description)

    def log_error_ner_vs_lemma_mismatch(self, id_data_text, id_sentence, words_array, lemms_array):
        err_description = "Ner's mismatch Lemma's. id_project = {} id_data_text = {} id_sentence = {}\n words_ner = {}\n lemmas = {}\n".\
              format(self.id_project, id_data_text, id_sentence, words_array, lemms_array)

        self.db.log_error("Ner's mismatch Lemma's Error", self.id_project, err_description)

