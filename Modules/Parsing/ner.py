import modules.common_mod.const as const
import modules.common_mod.common as common

from deeppavlov import configs, build_model

import re

from modules.common_mod.globvars import GlobVars
if const.PY_ENVIRONMENT: GD = None

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

ERROR_NER_512TOKENS = 'Too many NE in sentence'


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
        #self.re_pattern = re.compile(r'(?:(?:https?|ftp|file):(?://|\\)|www\.|ftp\.)[\S]{0,}\.[\S]{0,}')
        self.re_pattern = re.compile(r'(?:(?:https?|ftp|file):(?://|\\)|www\.|ftp\.)[\S]{0,}\.([\S](?<![\),])){0,}')

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

    def imitate_to_original(self, words_list, lemms_list, imitate_dict, ners_list):
        for i in range(0, len(words_list)):
            if words_list[i] in imitate_dict:
                lemms_list[i] = imitate_dict[words_list[i]]
                words_list[i] = imitate_dict[words_list[i]]
                ners_list[i] = ENTITY_NAME_URL
        

class NerConsolidator(common.CommonFunc):
    '''
        //ner or NE = Named Entity
        Consolidates NE-types consisting of two or more parts. Removes doubles of NE.
    '''
    def __init__(self, *args, **kwargs):
        
        super().__init__(*args, **kwargs)

        self.ne_types = [{ 'b': ENTITY_NAME_B_LOC, 'i': ENTITY_NAME_I_LOC, 'r': ENTITY_NAME_LOC },
                         { 'b': ENTITY_NAME_B_PER, 'i': ENTITY_NAME_I_PER, 'r': ENTITY_NAME_PER },
                         { 'b': ENTITY_NAME_B_ORG, 'i': ENTITY_NAME_I_ORG, 'r': ENTITY_NAME_ORG }
                        ]

        gvars = GlobVars(GD)

        self.not_entity_ne = []
        NER_ENT_TYPES = gvars.get('NER_ENT_TYPES')
        for key in NER_ENT_TYPES:
            if NER_ENT_TYPES[key]['not_entity']:
                self.not_entity_ne.append(key)

    def _init_work_lists(self):
        self._res_ner_dict = {} #dict format: {NE type: [ner1, ner2, ...]}

        self.res_ner_types_list = [] #an array containing data of keys from _res_ner_dict
        self.res_ner_list       = [] #an array containing data of ners from _res_ner_dict
        self.res_ner_idx        = [] #an array of length equal to the original with indexes of res_ner_list
        
        self._ne_b_t_list = []
        self._ne_i_t_list = []
        self._ne_b_list   = []
        self._ne_i_list   = []
        self._ne_idx_list = []

    def _get_cur_elements(self, i):
        self._ne_t = self._ner_types_list[i]
        self._ne   = self._ner_list[i]

    def _define_cur_ne_type(self):
        self._cur_ne_type = ''
        self._cur_ne_non_consolidated = True

        if self._ne_t == '' or self._ne_t in self.not_entity_ne:
            return

        #try to find consolidated-NE
        for t in self.ne_types:
            if self._ne_t == t['b'] or self._ne_t == t['i']:
                self._cur_ne_type = t['r']
                self._cur_ne_non_consolidated = False
                return
        
        #its non-consolidated NE
        self._cur_ne_type = self._ne_t
        return


    def _consolidate_if_necessary(self, the_end = False):
        if self._prev_ne_type is not None or the_end:
            if (self._cur_ne_type != self._prev_ne_type or the_end) \
              and (len(self._ne_b_list) > 0 or len(self._ne_i_list) > 0):
                cons_ner = " ".join(self._ne_b_list)
                if len(self._ne_i_list) > 0:
                    if len(self._ne_b_list) > 0:
                        cons_ner += " "
                    cons_ner += " ".join(self._ne_i_list)
                
                idx = self._add_cons_ner_if_not_exist(self._prev_ne_type, cons_ner)
                num_words_in_ner = len(self._ne_i_list)+len(self._ne_b_list)
                self.res_ner_idx.extend([idx for _ in range(0, num_words_in_ner)])
                
                self._ne_b_t_list.clear()
                self._ne_i_t_list.clear()
                self._ne_b_list  .clear()
                self._ne_i_list  .clear()
            #self._prev_ne_type = self._cur_ne_type
        
        if self._cur_ne_non_consolidated and not the_end:
            idx = self._add_cons_ner_if_not_exist(self._cur_ne_type, self._ne)
            self.res_ner_idx.append(idx)
            
        self._prev_ne_type = self._cur_ne_type

    def _add_cons_ner_if_not_exist(self, ner_type, consolidated_ner):
        if ner_type == '':
            return None

        if not ner_type in self._res_ner_dict:
            self._res_ner_dict[ner_type] = []

        if consolidated_ner in self._res_ner_dict[ner_type]:
            idx = self.res_ner_list.index(consolidated_ner)
        else:
            self.res_ner_types_list.append(ner_type)
            self.res_ner_list.append(consolidated_ner)
            self._res_ner_dict[ner_type].append(consolidated_ner)
            idx = len(self.res_ner_list)-1
        
        return idx

    def _store_ner_if_necessary(self):
        if self._cur_ne_non_consolidated:
            #return
            pass
        elif self._cur_ne_type == '':
            #self.res_ner_idx.append(None)
            pass
        else:
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

        return self.res_ner_types_list, self.res_ner_list, self.res_ner_idx

class MixedLettersDetector(common.CommonFunc):
    '''detect sentences consisting of words, mixed English and Russian letters 
    '''
    def __init__(self, *args, **kwargs):
        
        super().__init__(*args, **kwargs)

        self.percentage_of_mixed_words = 10
        #self.re_pattern_words = re.compile("[а-яА-Яa-zA-Z]{2,}")
        #self.re_pattern_mixed_words = re.compile("([а-яА-Я]{1,}[a-zA-Z]{1,}|[a-zA-Z]{1,}[а-яА-Я]{1,})")
        self.re_pattern_words = re.compile("[а-яa-z]{2,}",re.I)
        self.re_pattern_mixed_words = re.compile("([а-я]{1,}[a-z]{1,}|[a-z]{1,}[а-я]{1,})",re.I)

    def _get_tokens(self, sentence):
        return sentence.split(' ')

    def _get_words(self, tokens):
        return [w for w in filter(self.re_pattern_words.match, tokens)]

    def _get_mixed_words(self, words):
        return [w for w in filter(self.re_pattern_mixed_words.match, words)]

    def _is_meet_mixed_criterion(self, num_words, num_mixed_words):
        return num_mixed_words / num_words * 100 < self.percentage_of_mixed_words

    def is_sentence_mixed(self, sentence):
        tokens = self._get_tokens(sentence)
        self.num_tokens = len(tokens)

        words = self._get_words(tokens)
        num_words = len(words)
        if num_words == 0:
            return False

        mixed_words = self._get_mixed_words(words)
        num_mixed_words = len(mixed_words)
        if num_mixed_words == 0:
            return False

        if self._is_meet_mixed_criterion(num_words, num_mixed_words):
            return False

        return True

class DoubleSymbolsRemover(common.CommonFunc):
    '''remove double symbols from string 
    '''
    def __init__(self, *args, **kwargs):
        
        super().__init__(*args, **kwargs)

        self.symbols = u'-_&#$\u200b'
        self.len_repeating_block = 3
        self._initilize()

    def _initilize(self):
        self.repeating_block = []
        for s in self.symbols:
            self.repeating_block.append(s * self.len_repeating_block)

    def remove_from_str(self, original_str):
        for repeating_block in self.repeating_block:
            while repeating_block in original_str:
                original_str = original_str.replace(repeating_block, repeating_block[0])
        return original_str

    def remove_from_list(self, sentences):
        for i in range(len(sentences)):
            need_remove = False
            for repeating_block in self.repeating_block:
                if repeating_block in sentences[i]:
                    need_remove = True
                    break
            if need_remove:
                sentences[i] = self.remove_from_str(sentences[i])