import Common.const as const
import Common.common as common

import Parsing.token as token
import Parsing.sentiment as sentiment

import Crawling.exceptions as exceptions

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

        self.raw_text = {}

        self.sent_tokenizer = token.SentenceTokenizer()
        self.sentim_analizer = sentiment.SentimentAnalizer()

        self.debug_num_portions = 3 #number portions to stop process

    def get_text_portion(self):
        self.raw_text = self.db.data_text_select_unprocess(self.id_www_source, self.id_project, number_records = self.portion_size)

    def process(self):
        try:
            self._process()
        except exceptions.UserInterruptByDB as e:
            self.log_critical_error(e)
        except Exception as e:
            self.log_critical_error(e)
        pass

    def _process(self):
        portion_counter = 1
        self.db.log_trace('Get portion for DataProcess', self.id_project, 'Portion: {}'.format(portion_counter))
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
            self.db.log_trace('Get portion for DataProcess', self.id_project, 'Portion: {}'.format(portion_counter))
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

    def check_user_interrupt(self):
        if self.need_stop_checker is None:
            return False
        self.need_stop_checker.need_stop()

    def log_critical_error(self, raised_exeption):
        err_description = exceptions.get_err_description(raised_exeption, raw_text = str(self.raw_text))

        self.db.log_fatal(str(raised_exeption), self.id_project, err_description)


if __name__ == "__main__":
    if const.PY_ENVIRONMENT:
        import CrawlingPyOnly.plpyemul as plpyemul
        plpy = plpyemul.get_plpy()    
    if const.PY_ENVIRONMENT: 
        GD = None
    else: 
        GD = {}
    from Common.globvars import GlobVars
    gvars = GlobVars(GD)

    import Common.pginterface as pginterface

    cass_db = pginterface.MainDB(plpy, GD)
    need_stop_cheker = pginterface.NeedStopChecker(cass_db, 10, 'tokenize', state = 'off')

    dp = DataProcessor(db = cass_db,
                 id_project = 10,
                 id_www_source = 4,
                 need_stop_cheker = need_stop_cheker,
                 debug_mode = True,
                 msg_func = plpy.notice,
                 portion_size = 100)

    dp.debug_num_portions = 10

    dp.process()
    pass

