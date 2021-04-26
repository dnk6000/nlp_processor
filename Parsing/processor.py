import Common.const as const
import Common.common as common

import Parsing.token as token
import Parsing.sentiment as sentiment


class DataProcessor(common.CommonFunc):
    def __init__(self, *args,
                 db,
                 id_project,
                 id_www_source,
                 need_stop_cheker = None,
                 **kwargs):
        
        super().__init__(*args, **kwargs)

        self.db = db
        self.need_stop_checker = need_stop_cheker
        self.id_project = id_project
        self.id_www_source = id_www_source

        self.raw_text = {}

        self.sent_tokenizer = token.SentenceTokenizer()
        self.sentim_analizer = sentiment.SentimentAnalizer()

        self.debug_num_portions = 3 #number portions to stop process

    def get_text_portion(self):
        self.raw_text = self.db.data_text_select_unprocess(self.id_www_source, self.id_project, number_records = 50)

    def process(self):
        portion_counter = 1
        self.get_text_portion()

        while len(self.raw_text) > 0:
            self.check_user_interrupt()

            text_only = [i['content'] for i in self.raw_text]
        
            sentence_tokens = self.sent_tokenizer.tokenize(text_only)
            self.debug_msg('---entim text analize. portion = {}'.format(portion_counter))
            sentiment_text = self.sentim_analizer.analize(text_only)

            #TODO ускорить: собрать предложения в один список, проанализировать, разобрать
            sentiment_sent = []
            for sentences in sentence_tokens:
                self.debug_msg('sentim sent analize. ' + str(sentences))
                if len(sentences) == 0:
                    sentiment_sent.append(['skip'])
                else:
                    res = self.sentim_analizer.analize(sentences)
                    sentiment_sent.append(res)

            for txt_token in zip(self.raw_text, sentence_tokens, sentiment_sent):
                self.debug_msg('Upsert sentence token: id = {}  SENT: {}'.format(txt_token[0]['id'], txt_token[1]))
                self.save_token_result(txt_token[0]['id'], txt_token[1])

            if self.debug_mode and portion_counter >= self.debug_num_portions:
                break

            portion_counter += 1
            self.get_text_portion()
        pass

    def save_token_result(self, id_data_text, sentence_tokens):
        self.db.token_upsert_sentence(self.id_www_source, self.id_project, id_data_text, sentence_tokens)

    def save_sentim_text(self, id_data_text, rating):
        if rating == '':
            _rating = 0
        else:
            _rating = const.SENTIM_RATE[rating]
        self.db.sentiment_upsert_text(self.id_www_source, self.id_project, id_data_text, _rating) 

    def save_sentim_sentence(self, id_data_text, id_token_sentence, rating):
        if rating == '':
            _rating = 0
        else:
            _rating = const.SENTIM_RATE[rating]
        self.db.sentiment_upsert_sentence(self.id_www_source, self.id_project, id_data_text, id_token_sentence, _rating) 

    def check_user_interrupt(self):
        if self.need_stop_checker is None:
            return False
        self.need_stop_checker.need_stop()


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
                 msg_func = plpy.notice)

    dp.process()
    pass

