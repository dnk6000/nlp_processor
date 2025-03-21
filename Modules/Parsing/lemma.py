import modules.common_mod.const as const
import modules.common_mod.common as common

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

        #parsing raw_res
        raw_res_words = [_res.split('\n') for _res in raw_res]

        for lst_words in raw_res_words:
            res_word_list = []
            for wrd in lst_words:
                wrd_splitting = wrd.split('\t',3)
                if len(wrd_splitting) >= 3:
                    res_word_list.append({'word': wrd_splitting[1], 'lemma': wrd_splitting[2]})
            res.append(res_word_list)

        return res
