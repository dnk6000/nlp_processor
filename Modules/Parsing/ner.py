import Modules.Common.const as const
import Modules.Common.common as common

from deeppavlov import configs, build_model

class NerRecognizer(common.CommonFunc):
    def __init__(self, *args,
                 db = None,
                 id_project = None,
                 id_www_source = None,
                 need_stop_cheker = None,
                 dict_download = False,
                 **kwargs):
        
        super().__init__(*args, **kwargs)

        self.db = db
        self.need_stop_checker = need_stop_cheker
        self.id_project = id_project
        self.id_www_source = id_www_source

        self.ner_model = build_model(configs.ner.ner_rus_bert, download=dict_download)


    def recognize(self, list_sentences):
        return self.ner_model(list_sentences)


