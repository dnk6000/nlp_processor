import modules.common_mod.const as const
import modules.common_mod.common as common
import modules.common_mod.globvars as globvars

import modules.parsing.ner as ner
import modules.parsing.lemma as lemma
import modules.parsing.token as token
import modules.parsing.sentiment as sentiment

import modules.crawling.exceptions as exceptions

import modules.crawling.date as date

from modules.crawling.crawler import remove_empty_symbols

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

        self.process_description = 'non named process'


    def process(self):
        try:
            self.log_start_process();
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
    
    def log_start_process(self):
        self.debug_msg('{}: Batch start. Project: {}  {}'.format(date.date_now_str(), self.id_project, self.process_description))
        self.db.log_info(const.LOG_INFO_DATA_PROCESSOR, self.id_project, 'Started: '+self.process_description)

class SentimentProcessor(DataProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.raw_text = {}

        self.sent_tokenizer = token.SentenceTokenizer()
        self.sentim_analizer = sentiment.SentimentAnalizer()

        self.debug_num_portions = 3 #number portions to stop process

        self.process_description = 'Tonalization. Source: '+str(self.id_www_source)

    def get_text_portion(self):
        self.raw_text = self.db.data_text_select_unprocess(self.id_www_source, self.id_project, number_records = self.portion_size)

    def _process(self):
        portion_counter = 1
        self.debug_msg('{}: Get portion for SentimentProcessor Portion: {} Project: {}  {}'.format(date.date_now_str(), portion_counter, self.id_project, self.process_description))
        self.db.log_trace('Get portion for SentimentProcessor', self.id_project, 'Portion: {} {}'.format(portion_counter, self.process_description))
        self.get_text_portion()

        while len(self.raw_text) > 0:
            self.check_user_interrupt()

            #tokenize text to the sentences
            text_only = [i['content'] for i in self.raw_text]
            sentence_tokens = self.sent_tokenizer.tokenize(text_only)

            #sentiment analysis of the text as a whole
            #self.debug_msg('---sentim text analize. portion = {}'.format(portion_counter))
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

                #self.debug_msg('Upsert sentence token: id data text = {}  SENT: {}'.format(id_data_text, res_sentence_tokens))
                sent_token_id = self.save_token_result(id_data_text, res_sentence_tokens)

                #self.debug_msg('Upsert sentiment text: id data text = {}  RATE: {}'.format(id_data_text, rating_text))
                self.save_sentim_text(id_data_text, self.get_rating_id(rating_text))

                for sent_token in zip(res_sentence_tokens, res_sentiment_sent, sent_token_id):
                    rating_sent = sent_token[1]
                    id_token_sentence = sent_token[2]['upsert_sentence']
                    #self.debug_msg('Upsert sentiment sentence: id data text = {}  RATE: {} ID SENT TOKEN: {}'.format(id_data_text, rating_sent, id_token_sentence))
                    self.save_sentim_sentence(id_data_text, id_token_sentence, self.get_rating_id(rating_sent))

                self.db.data_text_set_is_process(id_data_text, autocommit = False)

                self.db.commit()

            if self.debug_mode and portion_counter >= self.debug_num_portions:
                break

            portion_counter += 1
            self.debug_msg('{}: Get portion for SentimentProcessor Portion: {} Project: {}  {}'.format(date.date_now_str(), portion_counter, self.id_project, self.process_description))
            self.db.log_trace('Get portion for SentimentProcessor', self.id_project, 'Portion: {} {}'.format(portion_counter, self.process_description))
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
        self.debug_sentence_id = 0
        self.debug_sent_id_list = []

        self.MAX_WORDS_IN_SENTENCE = 200 #150
        self.MAX_WORD_LEN = 1000

        self.url_recognizer = ner.UrlRecognizer(*args, **kwargs)

        self.loc_consolidator = ner.NerConsolidator(*args, **kwargs)

        self.mix_detector = ner.MixedLettersDetector(*args, **kwargs)

        self.double_symbols_remover = ner.DoubleSymbolsRemover(*args, **kwargs)

        self.process_description = 'NE recognize. Source: '+str(self.id_www_source)

    def get_raw_sentences(self):
        dbg_sent_id = 0
        dbg_sent_id_list = []
        if self.debug_mode:
            if self.debug_sentence_id == 0:
                dbg_sent_id_list = self.debug_sent_id_list
            else:
                dbg_sent_id = self.debug_sentence_id

        self.raw_sentences = self.db.sentence_select_unprocess(self.id_www_source, 
                                                               self.id_project, 
                                                               number_records = self.portion_size,
                                                               debug_sentence_id = dbg_sent_id,
                                                               debug_sentence_id_arr = dbg_sent_id_list)
        if self.debug_mode and len(self.debug_raw_sentences) > 0:
            for i in range(0,len(self.debug_raw_sentences)-1):
                if i > len(self.raw_sentences):
                    break
                self.raw_sentences[i]['txt'] = self.debug_raw_sentences[i]

    def _ner_safe_recognize(self, sentences):
        one_by_one_process = False
        try:
            ner_result = self.ner_recognizer.recognize(sentences)
            ner_errors = [None for _ in range(len(sentences))]
        except RuntimeError as e:
            if e.args[0] == "input sequence after bert tokenization shouldn't exceed 512 tokens.":
                one_by_one_process = True
            else:
                raise
        except Exception as e:
            raise

        if one_by_one_process:
            ner_result = ([],[])
            ner_errors = []
            for sentence in sentences:
                try:
                    _ner_result = self.ner_recognizer.recognize([sentence])
                    ner_result[0].append(_ner_result[0][0])
                    ner_result[1].append(_ner_result[1][0])
                    ner_errors.append(None)
                except RuntimeError as e:
                    if e.args[0] == "input sequence after bert tokenization shouldn't exceed 512 tokens.":
                        ner_result[0].append(None)
                        ner_result[1].append(None)
                        ner_errors.append(ner.ERROR_NER_512TOKENS)
                    else:
                        raise
                except Exception as e:
                    raise
        
        return ner_result, ner_errors

    def _process(self):
        portion_counter = 0

        while True:
            self.check_user_interrupt()

            portion_counter += 1
            self.debug_msg('{}: Get portion for NerProcessor Portion: {} Project: {}  {}'.format(date.date_now_str(), portion_counter, self.id_project, self.process_description))
            self.db.log_trace('Get portion for NerProcessor', self.id_project, 'Portion: {} {}'.format(portion_counter, self.process_description))
            self.get_raw_sentences()
            if len(self.raw_sentences) == 0:
                break

            self.remove_broken_sentences()

            if len(self.raw_sentences) > 0:

                self.debug_msg(f'   {date.date_now_str()} prepearing for ner & lemmatize...')
                _sentences = [i['txt'] for i in self.raw_sentences]
                self.double_symbols_remover.remove_from_list(_sentences)

                for i in range(len(_sentences)):
                    _sentences[i] = remove_empty_symbols(_sentences[i]) #to solving problem with \xa0

                self.url_recognizer.recognize(_sentences)
                if self.debug_sentence_id != 0:
                    self.debug_msg('    Sentences: '+str(_sentences))

                self.debug_msg(f'   {date.date_now_str()} doing lemmatizing operation...')
                lemma_result = self.lemmatizer.lemmatize(_sentences)
                self.debug_msg(f'   {date.date_now_str()} doing ner operation...')
                #ner_result = self.ner_recognizer.recognize(_sentences)
                ner_result, ner_errors = self._ner_safe_recognize(_sentences)

                self.debug_msg(f'   putting the results into db...')
                for result in zip(self.raw_sentences,           #0
                                  ner_result[0],                #1
                                  ner_result[1],                #2
                                  lemma_result,                 #3
                                  self.url_recognizer.result,   #4
                                  ner_errors                    #5
                                  ):
                    #record the sentence has been processed
                    id_data_text = result[0]['id_data_text']
                    id_sentence = result[0]['id']
                    self.save_set_is_process(id_sentence)

                    #check ne-recognition errors
                    ner_error = result[5]
                    if ner_error == ner.ERROR_NER_512TOKENS:
                        self.log_error_too_many_entity(id_data_text, id_sentence, result[0]['txt'])
                        continue

                    #record word tokens
                    words_array = result[1]        #get words tokens from ner processor
                    lemms_array = [_['lemma'] for _ in result[3]]
                    ners = result[2]
                    url_ners_array = result[4]

                    self.check_words_len(words_array, id_data_text, id_sentence, fix_error = True)
                    self.check_words_len(lemms_array, id_data_text, id_sentence, fix_error = False)

                    if len(words_array) != len(lemms_array):
                        _lemma_result = self.lemmatizer.lemmatize([' '.join([_ for _ in words_array])])
                        lemms_array = [_['lemma'] for _ in _lemma_result[0]]

                        if len(words_array) != len(lemms_array):
                            self.log_error_ner_vs_lemma_mismatch(id_data_text, id_sentence, words_array, lemms_array)
                            continue
                            #words_array = [_['word'] for _ in result[3]] #get words tokens from lemmatizer

                    self.url_recognizer.imitate_to_original(words_array, lemms_array, url_ners_array, ners)

                    ners_cons, lemms_array_cons, _ners_idx_array = self.loc_consolidator.consolidate(ners, lemms_array)

                    #record named entities
                    ners_type_id = self.convert_ners_to_id(ners_cons)

                    _ners_id = self.db.entity_upsert(self.id_www_source, self.id_project, id_data_text, id_sentence, ners_type_id, lemms_array_cons, autocommit = False)
                    ners_id = [_['id'] for _ in _ners_id]

                    ners_idx_array = []
                    for i in _ners_idx_array:
                        if i is None:
                            ners_idx_array.append(None)
                        else:
                            ners_idx_array.append(ners_id[i])
                    
                    #record words tokens
                    word_id_list = self.db.token_upsert_word(self.id_www_source, self.id_project, id_data_text, id_sentence, words_array, lemms_array, ners_idx_array, autocommit = False)

                    pass #end for result in zip(...)
                pass #end if len(self.raw_sentences) > 0

            self.debug_msg(f'   commiting...')
            self.db.commit()

            if self.debug_mode and portion_counter >= self.debug_num_portions:
                break

        pass


    def remove_broken_sentences(self):
        for i in range(len(self.raw_sentences)-1,-1,-1):
            if self.raw_sentences[i]['txt'] == '' or self.raw_sentences[i]['txt'].isspace():
                self.save_set_is_process(self.raw_sentences[i]['id'], True, token.SENT_BROKEN_TYPE_EMPTY)
                self.raw_sentences.pop(i)
            else:
                sentence_mixed = self.mix_detector.is_sentence_mixed(self.raw_sentences[i]['txt'])

                if sentence_mixed:
                    self.save_set_is_process(self.raw_sentences[i]['id'], True, token.SENT_BROKEN_TYPE_MIXED)
                    self.log_error_mixed_sentence(self.raw_sentences[i]['id_data_text'], self.raw_sentences[i]['id'], self.raw_sentences[i]['txt'])
                    self.raw_sentences.pop(i)
                elif self.mix_detector.num_tokens > self.MAX_WORDS_IN_SENTENCE:
                    self.save_set_is_process(self.raw_sentences[i]['id'], True, token.SENT_BROKEN_TYPE_TOO_LONG)
                    self.log_error_too_long_sentence(self.raw_sentences[i]['id_data_text'], self.raw_sentences[i]['id'], self.raw_sentences[i]['txt'])
                    self.raw_sentences.pop(i)


    def check_words_len(self, words_list, id_data_text, id_sentence, fix_error = True):
        for i in range(len(words_list)-1,-1,-1):
            if len(words_list[i]) > self.MAX_WORD_LEN:
                if fix_error:
                    self.log_error_too_long_word(id_data_text, id_sentence, words_list[i])
                words_list[i] = words_list[i][:self.MAX_WORD_LEN]
        #return words_list

    def save_set_is_process(self,id_sentence, is_broken = False, id_broken_type = None):
        self.db.sentence_set_is_process(id = id_sentence, is_broken = is_broken, id_broken_type = id_broken_type, autocommit = False)


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

    def log_error_too_long_sentence(self, id_data_text, id_sentence, txt):
        err_description = "Sentence is too long. id_project = {} id_data_text = {} id_sentence = {}\n txt = {}\n".\
              format(self.id_project, id_data_text, id_sentence, txt)

        self.db.log_error("Sentence is too long", self.id_project, err_description)

    def log_error_mixed_sentence(self, id_data_text, id_sentence, txt):
        err_description = "Sentence consists of words with mixed Russian-English letters. id_project = {} id_data_text = {} id_sentence = {}\n txt = {}\n".\
              format(self.id_project, id_data_text, id_sentence, txt)

        self.db.log_error("Sentence with mix-letters words", self.id_project, err_description)

    def log_error_too_long_word(self, id_data_text, id_sentence, txt):
        err_description = "Word is too long. id_project = {} id_data_text = {} id_sentence = {}\n txt = {}\n".\
              format(self.id_project, id_data_text, id_sentence, txt)

        self.db.log_error("Word is too long", self.id_project, err_description)

    def log_error_too_many_entity(self, id_data_text, id_sentence, txt):
        err_description = "Too many entities in sentence. id_project = {} id_data_text = {} id_sentence = {}\n txt = {}\n".\
              format(self.id_project, id_data_text, id_sentence, txt)

        self.db.log_error("Too many entities in sentence", self.id_project, err_description)


    def debug_sentence_id_list_one_by_one(self,sentence_id_list):
        for _sent_id in sentence_id_list:
            self.debug_msg('DEBUG Sentence id = '+str(_sent_id))
            self.debug_sentence_id = _sent_id
            self._process()

    def debug_sentence_id_list_whole(self,sentence_id_list):
        self.debug_msg('DEBUG Sentence id list = '+str(sentence_id_list))
        self.debug_sentence_id = 0
        self.debug_sent_id_list = sentence_id_list
        self._process()
