import Modules.Common.const as const
import Modules.Common.common as common

from deeppavlov import configs, build_model

import re

ENTITY_NAME_URL = 'GIT-URL'
ENTITY_NAME_B_LOC = 'B-LOC'
ENTITY_NAME_I_LOC = 'I-LOC'
ENTITY_NAME_LOC   = 'GIT-LOC'
ENTITY_NAME_B_PER = 'B-PER'
ENTITY_NAME_I_PER = 'I-PER'
ENTITY_NAME_PER   = 'GIT-PER'
ENTITY_NAME_B_ORG = 'B-ORG'
ENTITY_NAME_I_ORG = 'I-ORG'
ENTITY_NAME_ORG   = 'GIT-ORG'

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
                ners_list[i] = ENTITY_NAME_URL
        

class NerConsolidator(common.CommonFunc):
    def __init__(self, *args, **kwargs):
        
        super().__init__(*args, **kwargs)

        self.ne_types = [{ 'b': ENTITY_NAME_B_LOC, 'i': ENTITY_NAME_I_LOC, 'r': ENTITY_NAME_LOC },
                         { 'b': ENTITY_NAME_B_PER, 'i': ENTITY_NAME_I_PER, 'r': ENTITY_NAME_PER },
                         { 'b': ENTITY_NAME_B_ORG, 'i': ENTITY_NAME_I_ORG, 'r': ENTITY_NAME_ORG }
                        ]

    def _init_work_lists(self):
        self._res_ner_types_list = []
        self._res_ner_list = []
        self._ne_b_t_list = []
        self._ne_i_t_list = []
        self._ne_b_list = []
        self._ne_i_list = []

    def _get_cur_elements(self, i):
        self._ne_t = self._ner_types_list[i]
        self._ne   = self._ner_list[i]

    def _define_cur_ne_type(self):
        self._cur_ne_type = ''
        for t in self.ne_types:
            if self._ne_t == t['b'] or self._ne_t == t['i']:
                self._cur_ne_type = t['r']
                break

    def _consolidate_if_necessary(self, the_end = False):
        if self._prev_ne_type is not None or the_end:
            if (self._cur_ne_type != self._prev_ne_type or the_end) \
              and (len(self._ne_b_list) > 0 or len(self._ne_i_list) > 0):
                self._res_ner_types_list.append(self._prev_ne_type)
                self._res_ner_list      .append(" ".join(self._ne_b_list  ) + " ".join(self._ne_i_list  ))
                self._ne_b_t_list.clear()
                self._ne_i_t_list.clear()
                self._ne_b_list  .clear()
                self._ne_i_list  .clear()
        self._prev_ne_type = self._cur_ne_type

    def _store_ner_if_necessary(self):
        if self._cur_ne_type != '':
            for t in self.ne_types:
                if self._ne_t == t['b']:
                    self._ne_b_t_list.append(self._ne_t)
                    self._ne_b_list.append(self._ne)
                    break
                elif self._ne_t == t['i']:
                    self._ne_i_t_list.append(self._ne_t)
                    self._ne_i_list.append(self._ne)
                    break

    def consolidate(self, ner_types_list, ner_list):
        self._ner_types_list = ner_types_list
        self._ner_list = ner_list

        self._init_work_lists()
        self._prev_ne_type = None

        for i in range(0, len(ner_list)):
            self._get_cur_elements(i)

            self._define_cur_ne_type()

            self._consolidate_if_necessary()

            self._store_ner_if_necessary()
        
        self._consolidate_if_necessary(the_end = True)

        return self._res_ner_types_list, self._res_ner_list

    
