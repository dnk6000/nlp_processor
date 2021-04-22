import Common.const as const
import Common.common as common
import Common.pginterface as pginterface


class Tokenizer(common.CommonFunc):
    def __init__(self, *args, 
                 id_project,
                 id_www_source,
                 need_stop_cheker = None,
                 **kwargs):
        
        super().__init__(*args, **kwargs)

        self.need_stop_checker = need_stop_cheker
        self.id_project = id_project
        self.id_www_source = id_www_source


class SentenceTokenizer(Tokenizer):
    def __init__(self, *args, 
                 **kwargs):
        super().__init__(*args, **kwargs)



if __name__ == "__main__":
    if const.PY_ENVIRONMENT:
        import CrawlingPyOnly.plpyemul as plpyemul
        plpy = plpyemul.get_plpy()    

    cass_db = pginterface.MainDB(plpy, GD)
    need_stop_cheker = pginterface.NeedStopChecker(cass_db, id_project, 'tokenize', state = 'off')
    pass

