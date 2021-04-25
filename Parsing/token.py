import Common.const as const
import Common.common as common

from deeppavlov.models.tokenizers.ru_sent_tokenizer import RuSentTokenizer


class Tokenizer(common.CommonFunc):
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

    def get_text_portion(self):
        pass

    def tokenize(self):
        pass

    def process(self):
        pass

    def save_result(self):
        pass


class SentenceTokenizer(Tokenizer):
    def __init__(self, *args, 
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.tokenizer = RuSentTokenizer()

    def get_text_portion(self):
        self.raw_text = self.db.data_text_select_unprocess(self.id_www_source, self.id_project, number_records = 50)

    def process(self):
        self.get_text_portion()

        text_only = [i['content'] for i in self.raw_text]
        
        res = self.tokenizer(text_only)

        for txt_token in zip(self.raw_text, res):
            self.debug_msg('Upsert sentence token: id = {}  SENT: {}'.format(txt_token[0]['id'], txt_token[1]))

            self.db.token_upsert_sentence(self.id_www_source, self.id_project, txt_token[0]['id'], txt_token[1])

        pass

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

