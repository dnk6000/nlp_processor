import Modules.Common.const as const
import Modules.Common.common as common

from deeppavlov import configs, build_model

class Lemmatizer(common.CommonFunc):
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

        self.lemm_model = build_model(configs.morpho_tagger.UD2_0.morpho_ru_syntagrus_pymorphy_lemmatize, download=dict_download)


    def lemmatize(self, list_sentences):
        raw_res = self.lemm_model(list_sentences)

        res = []

        return res
