import Modules.Common.const as const
import Modules.Common.common as common

from deeppavlov import configs, build_model

import re

URL_ENTITY_NAME = 'GIT-URL'

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


class UrlRecognizer(common.CommonFunc):
    def __init__(self, *args, **kwargs):
        
        super().__init__(*args, **kwargs)

        #self.re_pattern = re.compile(r'/(?:(?:https?|ftp|file):\/\/|www\.|ftp\.)(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[-A-Z0-9+&@#\/%=~_|$?!:,.])*(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[A-Z0-9+&@#\/%=~_|$])/igm')
        #self.re_pattern = re.compile(r'(?:(?:https?|ftp|file):(?://|\\)|www\.|ftp\.)(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[-A-Z0-9+&@#\/%=~_|$?!:,.])*(?:\([-A-Z0-9+&@#\/%=~_|$?!:,.]*\)|[A-Z0-9+&@#\/%=~_|$])')
        self.re_pattern = re.compile(r'(?:(?:https?|ftp|file):(?://|\\)|www\.|ftp\.)[\S]{0,}\.[\S]{0,}')

        self.imitate_str = 'wwwaddr'
        self.result = []

    def recognize(self, sent_list):
        self.result = []
        for i in range(0, len(sent_list)):
            sent_list[i] = self._recognize(sent_list[i])  #change incoming list
            self.result.append(self.imitate_dict)
    
    def _recognize(self, txt):
        self.imitate_counter = 0
        self.imitate_dict = {}
        match = self.re_pattern.findall(txt)
        res = self.re_pattern.sub(self.re_sub_repl, txt)
        return res

    def re_sub_repl(self, match):
        res = self.imitate_str + str(self.imitate_counter)
        self.imitate_dict[res] = match[0]
        self.imitate_counter += 1
        return res

    def imitate_to_original(self, words_list, imitate_dict, ners_list):
        for i in range(0, len(words_list)):
            if words_list[i] in imitate_dict:
                words_list[i] = imitate_dict[words_list[i]]
                ners_list[i] = URL_ENTITY_NAME
        
