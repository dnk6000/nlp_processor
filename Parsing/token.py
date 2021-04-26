import Common.const as const
import Common.common as common

from deeppavlov.models.tokenizers.ru_sent_tokenizer import RuSentTokenizer


class Tokenizer(common.CommonFunc):
    def __init__(self, *args,
                 db = None,
                 id_project = None,
                 id_www_source = None,
                 need_stop_cheker = None,
                 **kwargs):
        
        super().__init__(*args, **kwargs)

        self.db = db
        self.need_stop_checker = need_stop_cheker
        self.id_project = id_project
        self.id_www_source = id_www_source

        self.raw_text = {}

    def get_text_portion(self):
        pass

    def tokenize(self):
        pass

    def process(self):
        pass

    def save_result(self):
        pass

    def check_user_interrupt(self):
        if self.need_stop_checker is None:
            return False
        self.need_stop_checker.need_stop()

class SentenceTokenizer(Tokenizer):
    def __init__(self, *args, 
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.tokenizer = RuSentTokenizer()
        self.debug_num_portions = 3 #number portions to stop process

    def get_text_portion(self):
        if self.id_www_source is not None and self.id_project is not None:
            self.raw_text = self.db.data_text_select_unprocess(self.id_www_source, self.id_project, number_records = 50)
        else:
            self.raw_text = {}

    def tokenize(self, list_text_only):
        return self.tokenizer(list_text_only)

    def process(self):
        portion_counter = 1
        self.get_text_portion()

        while len(self.raw_text) > 0:
            self.check_user_interrupt()

            text_only = [i['content'] for i in self.raw_text]
        
            res = self.tokenize(text_only)

            for txt_token in zip(self.raw_text, res):
                self.debug_msg('Upsert sentence token: id = {}  SENT: {}'.format(txt_token[0]['id'], txt_token[1]))
                self.save_result(txt_token[0]['id'], txt_token[1])

            if self.debug_mode and portion_counter >= self.debug_num_portions:
                break

            portion_counter += 1
            self.get_text_portion()

    def save_result(self, id_data_text, sentence_tokens):
        if self.db is not None:
            self.db.token_upsert_sentence(self.id_www_source, self.id_project, id_data_text, sentence_tokens)

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

    st = SentenceTokenizer(db = cass_db,
                 id_project = 10,
                 id_www_source = 4,
                 need_stop_cheker = need_stop_cheker,
                 debug_mode = True,
                 msg_func = plpy.notice)

    st.process()
    pass

