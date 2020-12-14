# -*- coding: utf-8 -*-

import re
import sys
import datetime
import time
import traceback
from itertools import combinations_with_replacement
from itertools import combinations
from itertools import product

import requests
import requests_html
import requests.exceptions
import lxml.html

import vk_requests
import vk_requests.exceptions

from bs4 import BeautifulSoup

#internal modules
import CrawlModulesPG.crawler as crawler
import CrawlModulesPG.scraper as scraper
import CrawlModulesPG.exceptions as exceptions
import CrawlModulesPG.const as const
import CrawlModulesPG.date as date

#import asyncio
#from pyppeteer import launch

""" 
class hierarchy:
    CrawlerSocialNet
	    CrawlerVk
		    CrawlerVkGroups
		    CrawlerVkWall
"""

class CrawlerSocialNet:

    def __init__(self, 
                 vk_account = {}, 
                 base_search_words = None, 
                 msg_func = None,
                 warning_func = None,
                 add_db_func = None, 
                 id_project = 0,
                 crawl_method = 'api',
                 **kwargs
                 ):
        """
        crawl_method = 'api' / 'browse'
        """

        self.id_project = id_project

        self.login     = '' if 'login' in vk_account else vk_account['login']
        self.password  = '' if 'password' in vk_account else vk_account['password']
        self.app_id    = '' if 'app_id' in vk_account else vk_account['app_id']

        self.request_tries = 3 #number of repeats requests in case of an error

        if base_search_words == None:
            self.base_search_words = ['пенза', 'penza', 'pnz']
        else:
            self.base_search_words = base_search_words
        
        self.search_list = []
        for i in self.base_search_words:
            self.search_list = [{ 'search_str': i,
                                 'type' : None, #Возможные значения: group, page, event
                                 'sort' : None,     #0 — сортировать по умолчанию (аналогично результатам поиска в полной версии сайта);
                                                    #1 — сортировать по скорости роста;
                                                    #2 — сортировать по отношению дневной посещаемости к количеству пользователей;
                                                    #3 — сортировать по отношению количества лайков к количеству пользователей;
                                                    #4 — сортировать по отношению количества комментариев к количеству пользователей;
                                                    #5 — сортировать по отношению количества записей в обсуждениях к количеству пользователей.
                                 '_level': 0,              #управляющий элемент
                                  '_source_search_str': i  #исходная поисковая строка, т.е. строка с _level = 0
                               }]

        self.crawl_method = crawl_method

        self.local_service_folder = 'C:\\Temp\\'

        #russian sequence to iterate
        ia = ord('а')
        ii = ord('й')
        iz = ord('я')
        self.alphabets = [''.join([chr(i) for i in range(ia,ii)]+[chr(i) for i in range(ii+1,iz+1)])]  #без букв й , ё
        #alph_str = ''.join([chr(i) for i in range(a,a+6)] + [chr(a+33)] + [chr(i) for i in range(a+6,a+32)]) # с буквами й , ё
        self.alphabets[0] += '\\'

        #english sequence to iterate
        a = ord('a')
        self.alphabets.append(''.join([chr(i) for i in range(a,a+26)]))

        self.msg_func = msg_func         # функция для сообщений
        self.warning_func = warning_func # функция для предупреждений
        self.add_db_func = add_db_func   # функция для добавления в БД

        self.api_request_pause_sec = 1  # пауза между запросами к API
        self.api_limit_res = 1000       # максимум записей возвращаемых API

        self.id_cash = []

        self._re_any_chars = re.compile('.*')



    def msg(self, message):

        if not self.msg_func == None:
            try:
                self.msg_func(message)
            except:
                pass

    def warning(self, message):

        if not self.warning_func == None:
            try:
                self.warning_func(message)
            except:
                pass
        else:
            self.msg_func('Warning:' + message)

    def _add_next_search_level(self, search_elem):
        if search_elem['_level'] == 0:
            #добавляем перебор по типу и сортировке
            for itype in ['group','page','event']:
                for isort in [0,1,2,3,4,5]:
                    self.search_list.append( { 'search_str': search_elem['_source_search_str'],
                                   'type' : itype,
                                   'sort' : isort,
                                   '_level' : 1,
                                   '_source_search_str' : search_elem['_source_search_str'],
                                })

        elif search_elem['_level'] == 1:
            #добавляем перебор по буквам русского и английского алфавита
            for alphabet in self.alphabets:
                for iletter in alphabet:
                    self.search_list.append( { 'search_str': search_elem['_source_search_str'] + ' ' + iletter,
                                    'type' : search_elem['type'],
                                    'sort' : search_elem['sort'],
                                    '_level' : 2,
                                    '_source_search_str' : search_elem['_source_search_str'],
                                })

        elif search_elem['_level'] == 2:
            #добавляем перебор по буквам - 2й уровень
            for alphabet in self.alphabets:
                for iletter in alphabet:
                    if not ' '+iletter in search_elem['search_str']:
                        self.search_list.append( { 'search_str': search_elem['search_str'] + ' ' + iletter,
                                        'type' : search_elem['type'],
                                        'sort' : search_elem['sort'],
                                        '_level' : 100,
                                        '_source_search_str' : search_elem['_source_search_str'],
                                    })

        elif search_elem['_level'] == 123456: #резерв
            #добавляем перебор по буквам русского и английского алфавита
            all_combinations = combinations(self.alphabets[0]+self.alphabets[1], 2)

            for comb in all_combinations:
                self.search_list.append( { 'search_str': search_elem['_source_search_str'] + ' '+ ' '.join(comb),
                                'type' : search_elem['type'],
                                'sort' : search_elem['sort'],
                                '_level' : 100,
                                '_source_search_str' : search_elem['_source_search_str'],
                            })


    def crawl_groups(self):
        res_for_pg = scraper.ScrapeResult()

        while len(self.search_list) > 0:
            search_elem = self.search_list.pop(0)
            #self._crawl_groups(search_elem)
            for res in self._crawl_groups(search_elem):
                yield res_for_pg.get_json_result(res)
            
        #for base_word in self.gen_base_search_words():
        #    for seq_num in [0,1]:
        #        self._crawl_groups(base_word, seq_num)

    def _crawl_groups(self, search_elem):
        
        self.msg('Поиск групп по строке поиска: '+search_elem['search_str']+'       Type: '+str(search_elem['type'])+'      Sort: '+str(search_elem['sort']))

        if self.crawl_method == 'api':
            res = self._crawl_groups_api(search_elem)
        else:
            res = self._crawl_groups_browse(search_elem)

        self._delete_cashed_id(res['groups_list'])
        
        #удаление сердечек смайликов и т.п. символов
        if len(res['groups_list']) > 0:
            for i in range(0,len(res['groups_list'])-1):
                res['groups_list'][i]['name']        = crawler.RemoveEmojiSymbols(res['groups_list'][i]['name'])
                res['groups_list'][i]['screen_name'] = crawler.RemoveEmojiSymbols(res['groups_list'][i]['screen_name'])
                res['groups_list'][i]['name']        = self._remove_unacceptable_symbols(res['groups_list'][i]['name'])
                res['groups_list'][i]['screen_name'] = self._remove_unacceptable_symbols(res['groups_list'][i]['screen_name'])
        
        #self._add_groups_to_db(res['groups_list'])
        yield res['groups_list']

        if res['count'] >= self.api_limit_res: #это максимум что выдает
            self.msg('Количество результатов: '+str(res['count'])+' превышает максимум: '+str(self.api_limit_res))
            self._add_next_search_level(search_elem)
        

    def _crawl_groups_api(self, search_elem):

        self.msg('Search groups by API: '+search_word)
        return { 'count': 0, 'groups_list': list() }

    def _crawl_groups_browse(self, search_elem):

        self.msg('Search groups by browser: '+search_word)
        return { 'count': 0, 'groups_list': list() }

    def _add_groups_to_db(self, groups_list):

        if self.add_db_func == None:
            self.msg('Add groups to BD //empty func//')
            return 

        for i in groups_list:
            self.add_db_func(i)

    def _delete_cashed_id(self, groups_list):
        
        numelem = len(groups_list)

        for i in range(numelem-1,-1,-1):
            if groups_list[i]['id'] in self.id_cash:
                groups_list.pop(i)
            else:
                self.id_cash.append(groups_list[i]['id'])
        
        self.msg('  new groups found: '+str(len(groups_list))+' / '+str(numelem))

    def _remove_unacceptable_symbols(self, old_str):
        '''perhaps the problem is related with psycopg2 library
        '''
        #new_str = old_str.replace("'", " ")
        new_str = old_str
        return new_str

class CrawlerVk(CrawlerSocialNet):
    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.msg('Инициализация')

        self.url = r'https://vk.com/'

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language':'ru-ru,ru;q=0.8,en-us;q=0.5,en;q=0.3',
            'Accept-Encoding':'gzip, deflate',
            'Connection':'keep-alive',
            'DNT':'1'
        }

        self.api_request_pause_sec = 2
        
        self.service_token = ''

        self.api_available = self.login != ''

        if self.api_available:
            with open(self.local_service_folder + 'vktoken.txt', 'r') as f:
                self.service_token = f.read()

            try:
                self.api = vk_requests.create_api(service_token = self.service_token)
                cities = self.api.database.getCities(country_id = 1, region_id = 1067455)
            except vk_requests.exceptions.VkAPIError:
                if self.get_vk_service_token():
                    self.api = vk_requests.create_api(service_token = self.service_token)
                else:
                    self.msg('Ошибка получения нового токена VK !')
                    raise 'Getting token error'
        #except Exception as e:
        #    self.msg(e)
        pass

    def _get_vk_session(self):

        #session = requests_html.session()
        session = requests_html.HTMLSession()
        data = session.get(self.url, headers=self.headers)
        page = lxml.html.fromstring(data.content)

        form = page.forms[0]
        form.fields['email'] = self.login
        form.fields['pass'] = self.password

        response = session.post(form.action, data=form.form_values())

        if 'onLoginDone' in response.text:
            return session
        else:
            self.msg('VK authorisation error !')
            return None

    def get_vk_service_token(self):

        self.msg('Получение нового токена')

        session = requests.session()
        data = session.get(self.url, headers = self.headers)
        page = lxml.html.fromstring(data.content)

        form = page.forms[0]
        form.fields['email'] = self.login
        form.fields['pass'] = self.password

        response = session.post(form.action, data=form.form_values())
        #self.msg('onLoginDone' in response.text)


        token_params = {'client_id': self.app_id, 
                  'display': 'page', 
                  'redirect_uri': 'https://oauth.vk.com/blank.html',
                  'scope': 'wall',
                  'response_type': 'token'
                  }
        
        req = session.get('https://oauth.vk.com/authorize', params = token_params)

        match = re.search(r'#access_token=.+&expires_in', req.url) 

        if match:
            self.service_token = req.url[match.start()+14 : match.end()-11]

            with open(self.local_service_folder + 'vktoken.txt', 'w') as f:
                psw = f.write(self.service_token)

            return True

        return False

class CrawlerVkGroups(CrawlerVk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _crawl_groups_api(self, search_elem):

        time.sleep(self.api_request_pause_sec) 

        params = { 'q'     : search_elem['search_str'],
                   'count' : self.api_limit_res
                  }

        if not search_elem['type'] == None:
            params['type'] = search_elem['type']

        if not search_elem['sort'] == None:
            params['sort'] = search_elem['sort']
        
        #groups = self.api.groups.search(q = search_word, count = self.api_limit_res)
        groups = self.api.groups.search(**params)
        
        return { 'count': groups['count'], 'groups_list': groups['items'] }

    def _scrape_first_page_searching_groups(self, htmltxt):
        
        groups_list = list()

        soup = BeautifulSoup(htmltxt, "html.parser")

        articleBodyTag     = 'groups_row search_row clear_fix'
        tagsArticleBody     = soup.findAll(self._re_any_chars, { 'class' : re.compile(articleBodyTag) })
        for tagBody in tagsArticleBody:
            
            self.count += 1
            #self.msg('group id '+tagBody.attrs['data-id'])

            tagBlocks = tagBody.findAll(re.compile('^img'), { 'class' : re.compile('search_item_img') })
            for tagBlock in tagBlocks:
                self.msg(str(self.count) + ' group id: '+tagBody.attrs['data-id'] + '     group name: '+tagBlock.attrs['alt'])
                groups_list.append({ 'id'   : tagBody.attrs['data-id'], 
                                     'name' : tagBlock.attrs['alt'],
                                     'screen_name' : '',
                                     'is_closed' : 0
                                   })

            tagLabeled = tagBody.findAll(self._re_any_chars, { 'class' : 'labeled ' })
            for tagLab in tagLabeled:
                self.msg(tagLab.content)


        self._add_groups_to_db(groups_list)

    def _scrape_fetch_page_searching_groups(self, htmltxt):
        
        groups_list = list()

        soup = BeautifulSoup(htmltxt, "html.parser")

        articleBodyTag     = 'groups_row search_row clear_fix'
        tagsArticleBody     = soup.findAll(self._re_any_chars, { 'class' : re.compile(articleBodyTag) })
        for tagBody in tagsArticleBody:
            
            self.count += 1
            #self.msg('group id '+tagBody.attrs['data-id'])

            tagBlocks = tagBody.findAll(re.compile('^img'), { 'class' : re.compile('search_item_img') })
            for tagBlock in tagBlocks:
                self.msg(str(self.count) + ' group id: '+tagBody.attrs['data-id'] + '     group name: '+tagBlock.attrs['alt'])
                groups_list.append({ 'id'   : tagBody.attrs['data-id'], 
                                     'name' : tagBlock.attrs['alt'],
                                     'screen_name' : '',
                                     'is_closed' : 0
                                   })

        self._add_groups_to_db(groups_list)

    def _get_serch_query_id(self, htmltxt):
        match = re.search(r'"query_id":"\d+"', htmltxt) 

        if match:
            self.serch_query_id = htmltxt[match.regs[0][0]+12:match.regs[0][1]-1]
        else:
            self.serch_query_id = 0
            self.msg('Getting search query id error !')

    def _crawl_groups_browse(self, search_word):

        self.count = 0
        _search_group_offset = 40

        session = self._get_vk_session()
 
        _params={'c[per_page]': str(_search_group_offset),
                 'c[q]'       : search_word,
                 'c[section]' : 'communities'
                }

        #url = r'https://vk.com/search?c%5Bper_page%5D=40&c%5Bq%5D=пенз&c%5Bsection%5D=communities'
        data = session.get(self.url+'search', headers=self.headers, params=_params)

        self._get_serch_query_id(data.text)

        data.html.render()

        self._scrape_first_page_searching_groups(data.text)

        _params_post = {'act': 'show_more', 
                      'al': '1',
                      'al_ad': '0',
                      'c[q]': search_word,
                      'c[section]': 'communities',
                      'offset': str(_search_group_offset),
                      'query_id': self.serch_query_id,
                      'real_offset': str(_search_group_offset)
                     }  
        
        for i in range(300):    #доделать остановку по окончании листинга, пока заложено 300 прокручиваний

            self.msg('_____________________ STEP __ ' + str(i))

            time.sleep(self.api_request_pause_sec)

            _params_post['offset']      = str(_search_group_offset)
            _params_post['real_offset'] = str(_search_group_offset)

            data = session.post(self.url+'search', headers=self.headers, params=_params, data=_params_post)
    
            _htmltxt = data.text.replace('\\', '')
    
            search_record_tag = 'groups_row search_row clear_fix'
            first_search_tag = '<div class="' + search_record_tag

            match = re.search(r'<!--.+?'+first_search_tag, _htmltxt) 

            if match:
                _htmltxt = _htmltxt[match.regs[0][1]-len(first_search_tag):]
            
            self._scrape_fetch_page_searching_groups(_htmltxt)

            _search_group_offset += 20

    def _add_groups_to_db(self, groups_list):

        self.msg(' !!!!!!!!!!!!!!!!!!!!! NOT USE !!!!!!!!!!!!!!!!')
        #n = len(groups_list)
        #c = 0
        #self.msg('Add groups to DB: ' + str(n))

        #for Gr in groups_list:
        #    c += 1
        #    self.msg('Add groups to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(Gr['id']) + ' ' + Gr['name'])
        #    self.add_db_func(self.id_project,
        #                     'vk',
        #                     'group',
        #                     Gr['id'],
        #                     self._remove_unacceptable_symbols(Gr['name']),
        #                     self._remove_unacceptable_symbols(Gr['screen_name']),
        #                     Gr['is_closed'] == 1
        #    )
        return


class SnRecrawlerCheker:
    def __init__(self, cass_db = None, id_www_sources = None, id_project = None, sn_id = None, recrawl_days_post = None, recrawl_days_reply = None, plpy = None):
        self.post_reply_dates = dict()
        self.group_upd_date = const.EMPTY_DATE
        self.group_last_date = const.EMPTY_DATE  #last activity date
        self.str_to_date = date.StrToDate('%Y-%m-%d %H:%M:%S+.*')

        if cass_db != None:
            res = cass_db.get_sn_activity(id_www_sources, id_project, sn_id, recrawl_days_post)

            _td = datetime.timedelta(days=recrawl_days_reply)

            for i in res:
                _post_id = i['sn_post_id']
                _upd_date = self._get_date(i['upd_date'])
                if _post_id == '':
                    self.group_upd_date = _upd_date
                    self.group_last_date = self._get_date(i['last_date'])
                else:
                    #if _upd_date == None or _upd_date == const.EMPTY_DATE:
                    #    self.post_reply_dates[i['sn_post_id']] = { 'upd_date': i['last_date'], 'wait_date': i['last_date'] - _td } #at the initial stage, only. 'upd_date' is not filled in
                    #else:
                    self.post_reply_dates[i['sn_post_id']] = { 'upd_date': _upd_date, 'wait_date': _upd_date - _td }

    def _get_date(self, dt):
        if type(dt) == str:
            return self.str_to_date.get_date(dt, type_res = 'D')
        else:
            return dt

    def is_crawled_post(self, dt):
        '''is post already crawled ? True / False '''
        if dt == const.EMPTY_DATE or self.group_upd_date == const.EMPTY_DATE:
            return False
        return dt < self.group_upd_date

    def is_reply_out_of_wait_date(self, post_id, dt):
        if not post_id in self.post_reply_dates or dt == const.EMPTY_DATE:
            return False
        return dt < self.post_reply_dates[post_id]['wait_date']

    def is_reply_out_of_upd_date(self, post_id, dt):
        if not post_id in self.post_reply_dates or dt == const.EMPTY_DATE:
            return False
        return dt < self.post_reply_dates[post_id]['upd_date']

    def get_post_out_of_date(self, post_id, date_type):
        ''' for test purpose only'''
        if not post_id in self.post_reply_dates:
            return const.EMPTY_DATE
        else:
            return self.post_reply_dates[post_id][date_type]

    def get_post_list(self):
        return [i for i in self.post_reply_dates]

class CrawlerVkWall(CrawlerVk):

    def __init__(self, *args, 
                 subscribers_only = False, 
                 date_deep = const.EMPTY_DATE, 
                 sn_recrawler_checker = None,
                 need_stop_cheker = None,
                 **kwargs
                 ):
        
        super().__init__(*args, **kwargs)
        
        self._cw_subscribers_only = subscribers_only

        self._cw_date_deep = date_deep


        self._cw_define_tags()

        self._cw_wall_fetch_par = {
                    'act': 'get_wall', 
                    'al': '1',
                    'fixed': '',
                    'offset': '',
                    'onlyCache': 'false',
                    'owner_id': '',
                    'type': 'own',
                    'wall_start_from': '',
                    }  

        self._str_to_date = date.StrToDate(['dd mmm в hh:mm', 'dd mmm yyyy', 'сегодня в hh:mm', 'вчера в hh:mm'], url = self.url, msg_func = self.msg_func)

        if sn_recrawler_checker == None:
            self._sn_recrawler_checker = SnRecrawlerCheker() 
        else:
            self._sn_recrawler_checker = sn_recrawler_checker

        self._cw_need_stop_checker = need_stop_cheker

        self._cw_set_debug_mode(turn_on = False)

        if 'write_file_func' in kwargs:
            self._write_file_func = kwargs['write_file_func']
        else:
            self._write_file_func = None

    def _cw_define_tags(self):

        TN = scraper.TagNode
        TT = scraper.TagTree

        NoneFunc = None
        NoneFuncParam = None
        OneTag = False    #for soup.find()
        MultiTag = True   #for soup.findAll()

        rc = lambda z: { 'class' : re.compile(z) } 
        ro = lambda z: { 'onclick' : re.compile(z) }
        rsum = lambda z1, z2: z1.update(z2)

        fn = self._cw_find_tags
        fnpl = self._cw_find_tags_in_post_list
        fnrpl = self._cw_find_tags_repl_list
        pr = lambda p1, p2, p3: { 'tag_key':p1, 'multi': p2, 'mode': p3, 'deep_parent': '' }

        nPostTag = rc('^_post post.*')
        nDateTag = rc('^rel_date')
        nAuthorTag = rc('^author')
        
        self._cw_tg_NotFound = TT ( [ TN( fn, self._cw_check_not_found , pr(rc('message_page_title'), OneTag, 'tag 1') ),
                                      TN( fn, self._cw_check_not_found , pr(rc('message_page_body') , OneTag, 'tag 2') )
                                    ]
                                  )

        self._cw_tg_Subscribers = TT ( TN( fn, None , pr( { 'aria-label' : re.compile('(Подписчики)|(Участники)') }, OneTag  , '-' ) ) )
        #self._cw_tg_Subscribers.add  (    TN( fn, self._cw_scrap_subscribers , pr( { 'class' : 'header_count fl_l' }, OneTag, 'number') ) )
        self._cw_tg_Subscribers.add  (    TN( fn, self._cw_scrap_subscribers , pr( { 'class' : re.compile('(header_count)|(group_friends_count)') }, OneTag, 'number') ) )

        self._cw_tg_FixedArea = TT ( TN( fn, self._cw_scrap_fixed_area , pr(rc('wall_fixed'), OneTag  , 'all fixed area'     ) ) )
        self._cw_tg_FixedArea.add  (    TN( fn, self._cw_scrap_fixed_area , pr(nPostTag        , MultiTag, 'posts in fixed area') ) )
        #self._cw_tg_FixedArea.childs[0].add  (    TN( fn  , self._cw_scrap_posts , pr(nAuthorTag      , OneTag   , 'author'    ) ) )
        #self._cw_tg_FixedArea.childs[0].add  (    TN( fn  , self._cw_scrap_posts , pr(nDateTag        , OneTag   , 'date'      ) ) )
        #self._cw_tg_FixedArea.childs[0].add  (    TN( fnpl, self._cw_scrap_posts , pr(rc('wall_text') , MultiTag , 'wall texts') ) )
        
        self._cw_tg_Posts = TT ( TN( fn, self._cw_scrap_posts , pr(nPostTag        , MultiTag  , 'posts list') ) )
        self._cw_tg_Posts.add  (    TN( fn  , self._cw_scrap_posts , pr(nAuthorTag      , OneTag   , 'author'    ) ) )
        self._cw_tg_Posts.add  (    TN( fn  , self._cw_scrap_posts , pr(nDateTag        , OneTag   , 'date'      ) ) )
        self._cw_tg_Posts.add  (    TN( fnpl, self._cw_scrap_posts , pr(rc('wall_text') , MultiTag , 'wall texts') ) )
        #self._cw_tg_Posts.add  (    self._cw_tg_Replies)
        #self._cw_tg_Posts.add  (    TN( fn , self._cw_add_list_post_repl , pr({**ro('^return wall.showNextReplies'), **rc('replies_next_main')} , OneTag  , '') ) )

        self._cw_tg_Replies = TT ( TN( fn, self._cw_scrap_replies , pr(rc('^reply reply_dived') , MultiTag, 'repl dived') ) )
        self._cw_tg_Replies.add  (    TN( fn   , self._cw_scrap_replies , pr(nAuthorTag             , OneTag  , 'author'   ) ) )
        self._cw_tg_Replies.add  (    TN( fn   , self._cw_scrap_replies , pr(nDateTag               , OneTag  , 'date'     ) ) )
        self._cw_tg_Replies.add  (    TN( fnrpl, self._cw_scrap_replies , pr(rc('^wall_reply_text') , OneTag  , 'repl text') ) )
        #self._cw_tg_Replies.add  (    TN( fn   , self._cw_add_list_post_repl , pr({**ro('^return wall.showNextReplies'), **rc('replies_next|replies_prev')} , OneTag  , '') ) )
        #self._cw_tg_Replies.add  (    TN( fn   , self._cw_add_list_post_repl , pr({**ro('^return wall.showNextReplies')} , OneTag  , '') ) )

        self._cw_tg_ShowPrevRepl = TT ( TN( fn, self._cw_scrap_repl_show_next , pr(rc('^replies_wrap_deep') , MultiTag, 'wrap deep') ) )
        self._cw_tg_ShowPrevRepl.add  (    TN( fn, self._cw_scrap_repl_show_next , pr(ro('^return wall.showNextReplies|^return Unauthorized.showMoreBox')   , OneTag  , 'show next') ) )
        
    def _cw_set_debug_mode(self, turn_on = False, debug_post_filter = '', num_fetching_post = 999999):
        self._cw_debug_num_fetching_post = num_fetching_post #number of fetching wall page
        self._cw_debug_post_filter = debug_post_filter
        self._cw_debug_mode = turn_on

        if self._cw_debug_mode: 
            self.msg('! Debug mode ! _cw_debug_post_filter = '+self._cw_debug_post_filter+'    _cw_debug_num_fetching_post = '+str(self._cw_debug_num_fetching_post))

    def crawl_wall(self, group_id): # class variables prefix: _cw_
        try:
            for step_result in self._crawl_wall(group_id):
                yield step_result
        except requests.exceptions.RequestException as e:
            _descr = exceptions.get_err_description(e, _cw_url = self._cw_url)
            self._cw_add_to_result_critical_error(str(e), _descr)
            yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)  
            return
        except exceptions.UserInterruptByDB as e:
            _descr = exceptions.get_err_description(e, _cw_url = self._cw_url)
            self._cw_add_to_result_critical_error(str(e), _descr, stop_process = True)
            yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)  
            return
        except Exception as e:
            _descr = exceptions.get_err_description(e, _cw_url = self._cw_url)
            self._cw_add_to_result_critical_error(str(e), _descr)
            yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)  
            return
            
    def _crawl_wall(self, group_id):
        self._cw_num_subscribers = 0
        self._cw_fixed_post_id = ''
        self._cw_post_counter = 0
        self._cw_post_counter2 = 0    # equal to _cw_post_counter except for fixed posts
        self._cw_post_repl_list = []  # first level replies
        self._cw_post_repl2_list = [] # second level replies = href 'Показать предыдущие комментарии'
        self._cw_scrape_result = []
        self._cw_res_for_pg = scraper.ScrapeResult()
        self._cw_fetch_post_counter = 0
        self._cw_noncritical_error_counter = {}
        self._cw_last_dates_activity = {}
        self._cw_group_prev_last_date_activity = const.EMPTY_DATE
        self._cw_group_last_date_activity = self._sn_recrawler_checker.group_last_date

        self._cw_num_posts_request = 10  #number of posts per one fetch-request
        self._cw_num_repl_request = 20  #number of replies received per request

        self._cw_session = requests_html.HTMLSession()

        self._cw_group_id = group_id
        if not self._cw_debug_mode or self._cw_debug_post_filter == '':
            self._cw_url = self.url + 'club' + self._cw_group_id  #self.url + 'wall-' + group_id
        else:
            self._cw_url = self.url + 'wall-' + self._cw_group_id+'_'+self._cw_debug_post_filter  #addressing to a specific post: https://vk.com/wall-16758516_109038

        self._cw_url_fetch = self.url + 'al_wall.php'
        if self._cw_debug_mode:
            self.msg('URL:  '+self._cw_url)

        self._check_user_interrupt()

        _request_attempt = self.request_tries
        while True:
            try:
                d = self._cw_session.get(self._cw_url, headers = self.headers)
                break
            except Exception as e:
                _request_attempt -= 1
                if _request_attempt == 0:
                    raise
                else:
                    self._cw_add_to_result_noncritical_error(const.ERROR_REQUEST_GET, self._cw_get_post_repr())
                    yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)

        self._write_debug_file(d.text)

        if not self._is_good_status_code(d.status_code, get_url = self._cw_url, stop_if_broken = True):
            self._cw_scrape_result.append( {'result_type': const.CW_RESULT_TYPE_NUM_SUBSCRIBERS, 
                                            'sn_id': self._cw_group_id,  
                                            'num_subscribers': 0,
                                            'is_broken': True,
                                            'broken_status_code': str(d.status_code)
                                           } )  
            yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)
            return

        self._cw_soup = BeautifulSoup(d.text, "html.parser")

        #DEBUG
        #debug_file = open(r"C:\Work\Python\CasCrawl37\Test\tst.txt", encoding='utf-8')
        #msg = debug_file.read()
        #debug_file.close()
        #self._cw_soup = BeautifulSoup(msg, "html.parser")
        #DEBUG


        if not self._cw_subscribers_only:
            self._cw_scrape_result.append( {'result_type': const.CW_RESULT_TYPE_HTML, 'url': self._cw_url, 'content': d.text } ) #!!!!!!!!!!!! d.content ??
        
        #is group found ?
        self._cw_signs_count = 0
        self._cw_tg_NotFound.scan(self._cw_soup, {})
        if self._cw_signs_count >= 1: #== 2: #two signs that the group was not found
            self._cw_scrape_result.clear()
            self._cw_scrape_result.append( {
                'result_type': const.CW_RESULT_TYPE_FINISH_NOT_FOUND, 
                'datetime': date.date_to_str(datetime.datetime.now()),
                'event_description': self._cw_get_post_repr()
                } )
            yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)
            return

        #get subscribers
        self._cw_tg_Subscribers.scan(self._cw_soup, {})
        
        #DEBUG
        self.msg(str(self._cw_scrape_result[1]))
        #DEBUG

        if self._cw_subscribers_only:
            self._cw_scrape_result.append( {
                'result_type': const.CW_RESULT_TYPE_FINISH_SUCCESS if self._cw_num_subscribers > 0 else const.CW_RESULT_TYPE_NUM_SUBSCRIBERS_NOT_FOUND, 
                'datetime': date.date_to_str(datetime.datetime.now()),
                'event_description': self._cw_get_post_repr()
                } )
            yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)
            return

        #get fixed posts
        self._cw_tg_FixedArea.scan(self._cw_soup, {})

        self._stop_post_scraping = False
        _fetch_enable = True

        while _fetch_enable and not self._stop_post_scraping:
            self._check_user_interrupt()
            self._cw_fetch_post_counter = 0

            #get posts
            self._cw_tg_Posts.scan(self._cw_soup, {})
            if not self._scrape_result_empty():
                yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)

            if self._stop_post_scraping:
                self._cw_post_repl_list.extend(self._sn_recrawler_checker.get_post_list())  

            self._check_user_interrupt()
            for step in self._cw_get_post_replies():
                self._check_user_interrupt()
                if not self._scrape_result_empty():
                    yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)
            for step in self._cw_get_post_replies2(): #second level
                self._check_user_interrupt()
                if not self._scrape_result_empty():
                    yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)

            self._cw_trans_last_dates_to_scrape_result()

            #fetch browser page
            self._cw_debug_num_fetching_post -= 1
            if (self._cw_fetch_post_counter >= self._cw_num_posts_request) and (self._cw_debug_num_fetching_post > 0) and not self._stop_post_scraping: 
                for attempt in self._cw_fetch_wall():
                    self._check_user_interrupt()
                    if not self._scrape_result_empty():
                        yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)
            else:
                _fetch_enable = False

        self._cw_scrape_result.append( {
            'result_type': const.CW_RESULT_TYPE_FINISH_SUCCESS, 
            'datetime': date.date_to_str(datetime.datetime.now()),
            'event_description': self._cw_get_post_repr()
            } )
        yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)
        return


    def _cw_get_post_replies(self):
        '''request replies for the posts from '_cw_post_repl_list' and scrape results 
        '''
        while len(self._cw_post_repl_list) > 0:
            _post_id = self._cw_post_repl_list.pop(0)
            _first_step = True
            _replies_offset = 0
            _fetch_count = 0
            self._cw_fetch_repl_counter = 0
            self._stop_repl_scraping = False

            while (self._cw_fetch_repl_counter >= self._cw_num_repl_request or _first_step) and not self._stop_repl_scraping:
                _first_step = False

                par_data = {'act': 'get_post_replies', 
                                'al': '1',
                                'count': str(self._cw_num_repl_request),
                                'item_id': _post_id,
                                'offset': str(_replies_offset),
                                'order': 'desc',
                                'owner_id': '-'+self._cw_group_id#,
                                #'prev_id': str(_replies_prev_id),
                                #'top_replies': _replies_top_replies
                                } 
            
                _replies_offset += self._cw_num_repl_request
                
                if self._cw_debug_mode:
                    self.msg('############################################################')
                    self.msg('REPLY FETCH.  _post_id = '+_post_id+' _cw_fetch_repl_counter = '+str(self._cw_fetch_repl_counter)+' _fetch_count = '+str(_fetch_count))
                    self.msg(self._cw_url)
                    self.msg(par_data)
                    self.msg('############################################################')

                for attempt_res in self._cw_fetch(par_data, 'Error when trying to fetch post replies ! '+str(par_data)):
                    if attempt_res != 200:  #TODO constant
                        yield _fetch_count

                self._cw_fetch_repl_counter = 0
                self._cw_tg_Replies.scan(self._cw_soup, {'post_id': _post_id})

                self._cw_tg_ShowPrevRepl.scan(self._cw_soup, {'post_id': _post_id})  #addition ShowPrev-list

                if self._cw_debug_mode:
                    if self._stop_repl_scraping:
                        self.msg(' --- set Repl STOP SCRAPING ---')

                _fetch_count += 1

                yield _fetch_count

    def _cw_get_post_replies2(self):
        '''press to href 'Показать следующие комментарии' and scrape request results 
        '''
        while len(self._cw_post_repl2_list) > 0:
            _list_elem = self._cw_post_repl2_list.pop(0)
            _first_step = True
            _replies_offset = int(_list_elem['offset'])
            _fetch_count = 0
            self._cw_fetch_repl_counter = 0
            self._stop_repl2_scraping = False

            while (self._cw_fetch_repl_counter >= self._cw_num_repl_request or _first_step) and not self._stop_repl2_scraping:
                _count = int(_list_elem['count']) if _first_step else self._cw_num_repl_request

                par_data = {'act': 'get_post_replies', 
                                'al': '1',
                                'count': str(_count), #str(self._cw_num_repl_request),
                                'item_id': _list_elem['item_id'],
                                'offset': str(_replies_offset),
                                'order': 'desc',
                                'owner_id': '-'+self._cw_group_id#,
                                #'prev_id': str(_replies_prev_id),
                                #'top_replies': _replies_top_replies
                                } 
            
                _first_step = False

                _replies_offset += _count

                if self._cw_debug_mode:
                    self.msg('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                    self.msg('REPLY to REPLY FETCH.  _post_id = '+_list_elem['item_id']+' _cw_fetch_repl_counter = '+str(self._cw_fetch_repl_counter)+' _fetch_count = '+str(_fetch_count))
                    self.msg(self._cw_url)
                    self.msg(par_data)
                    self.msg('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                
                for attempt_res in self._cw_fetch(par_data, 'Error when trying to fetch 2nd-level post replies ! '+str(par_data)):
                    if attempt_res != 200:  #TODO constant
                        yield _fetch_count

                self._cw_fetch_repl_counter = 0
                self._cw_tg_Replies.set_par('deep_parent', _list_elem['item_id'])
                self._cw_tg_Replies.scan(self._cw_soup, {'post_id': _list_elem['post_id']})

                if self._cw_debug_mode:
                    if self._stop_repl2_scraping:
                        self.msg(' --- set Reply to Reply STOP SCRAPING ---')

                _fetch_count += 1

                yield _fetch_count

    def _cw_fetch(self, par_data, err_txt = ''):  
        '''make post-request with par_data params.
           either gets data or raise an exception 
        '''
        time.sleep(2)   #TODO !!!!!!!!!! smart func needed !
        
        _request_attempt = self.request_tries
        while True:
            try:
                #self._cw_session.encoding = 'UTF-8'
                d = self._cw_session.post(self._cw_url_fetch, headers = self.headers, data = par_data)
                break
            except Exception as e:
                _request_attempt -= 1
                if _request_attempt == 0:
                    raise
                else:
                    self._cw_add_to_result_noncritical_error(const.ERROR_REQUEST_POST, self._cw_get_post_repr()+err_txt)
                    yield _request_attempt
        
        #d.encoding = 'UTF-8'
        txt = d.text.replace('\\', '')
        #txt = txt.replace("'", "")
        txt = txt.replace('<!--{"payload":[0,["', '')
        if txt[0] != '<':
            txt = '<' + txt
        self._cw_scrape_result.append( {'result_type': const.CW_RESULT_TYPE_HTML, 'url': self._cw_url, 'content': d.text } )

        #self.msg(txt)

        self._cw_soup = BeautifulSoup(txt, "html.parser")
        return 200  #TODO need const

    def _cw_fetch_wall(self):
        self._cw_wall_fetch_par['fixed'] = self._cw_fixed_post_id
        self._cw_wall_fetch_par['owner_id'] = '-'+self._cw_group_id
        self._cw_wall_fetch_par['offset'] = str(self._cw_post_counter)
        self._cw_wall_fetch_par['wall_start_from'] = str(self._cw_post_counter2)

        for attempt in self._cw_fetch(self._cw_wall_fetch_par, 'Error when trying to fetch the wall !'):
            yield attempt


    def _cw_get_post_id(self, html_attr, error_msg = '', mode = ''):
        if mode == 'href':
            #match = re.match( r'(.*)-(\d*)_(\d*)', html_attr ) #'showNextReplies'
            match = re.match( r'.*(wall)-(\d*)_(\d*)\?reply=(\d*)', html_attr ) 
        else:
            match = re.match( r'^(post|replies|\/wall)-(\d*)_(\d*)', html_attr ) 

        if match:
            if mode == 'href':
                return {'group_id':match.group(2), 'post_id': match.group(3), 'parent_id': match.group(4)}
            else:
                return {'group_id':match.group(2), 'post_id': match.group(3) }
        else:
            raise exceptions.CrawlVkByBrowserError(self._cw_url, error_msg, self.msg_func)



    def _cw_find_tags(self, soup, par, **kwargs):
        if soup == None: return None, par

        if kwargs['multi']:
            res = soup.findAll(self._re_any_chars, kwargs['tag_key'])
        else:
            res = soup.find(self._re_any_chars, kwargs['tag_key'])
            
        return res, par

    def _cw_find_tags_in_post_list(self, soup, par, **kwargs):
        '''finds tags inside tag ~'post list'
        '''
        if soup == None: return None, par

        z = self._cw_get_post_id(soup.attrs['id'], 'Post item_id not found !')

        par['post_id'] = z['post_id']
        par['is_fixed_post'] = 'post_fixed' in soup.attrs['class']

        #Debug
        if self._cw_debug_mode and self._cw_debug_post_filter != '':
            if par['post_id'] != self._cw_debug_post_filter: 
                self._cw_post_counter += 1
                self._cw_post_counter2 += 1
                self._cw_fetch_post_counter += 1
                return None, par

        return self._cw_find_tags(soup, par, **kwargs)

    def _cw_find_tags_repl_list(self, soup, par, **kwargs):
        if soup == None: return None, par

        z = self._cw_get_post_id(soup.attrs['id'], 'Reply item_id not found !')

        par['reply_id'] = z['post_id']

        if not 'class' in soup.parent.attrs or not ('replies_list_deep' in soup.parent.attrs['class']): 
            par['parent_id'] = par['post_id']
            self._cw_fetch_repl_counter += 1
        else:
            z = self._cw_get_post_id(soup.parent.attrs['id'], 'Reply parent item_id not found !')
            par['parent_id'] = z['post_id']

        return self._cw_find_tags(soup, par, **kwargs)



    def _cw_scrap_fixed_area(self, result, par, **kwargs):
        if result == None: return None, par

        if kwargs['mode'] == 'all fixed area':
            return result, par

        else: #mode == 'posts in fixed area'
            if len(result) > 0:
                if len(result) > 1:
                    raise exceptions.CrawlVkByBrowserError(self._cw_url, 'Several fixed posts were found !', self.msg_func)

                z = self._cw_get_post_id(result[0].attrs['id'], 'Fixed post item_id not found !')

                self._cw_fixed_post_id = z['post_id']

                self._cw_post_counter -= 1

                return result, par

        return None, par

    def _cw_check_not_found(self, result, par, **kwargs):

        if result == None: return None, par

        if kwargs['mode'] == 'tag 1':
            if 'Ошибка' in result.text:
                self._cw_signs_count += 1
        if kwargs['mode'] == 'tag 2':
            if 'Сообщество не найдено' in result.text:
                self._cw_signs_count += 1
        return result, par

    def _cw_scrap_posts(self, result, par, **kwargs):
        if result == None: return None, par

        if kwargs['mode'] == 'posts list':
            
            if len(result) > 0 and (self._cw_group_id == ''):
                z = self._cw_get_post_id(result[0].attrs['id'], 'Group id not found !')

                self._cw_group_id = z['group_id']

            return result, par

        elif kwargs['mode'] == 'author':
            par['author'] = crawler.remove_empty_symbols(result.text)

        elif kwargs['mode'] == 'date':
            par['date'] = crawler.remove_empty_symbols(result.text)

        elif kwargs['mode'] == 'wall texts':
            if len(result) > 0:
                self._cw_post_counter += 1
                self._cw_post_counter2 += 1
                self._cw_fetch_post_counter += 1

                if len(result) > 1:
                    raise exceptions.CrawlVkByBrowserError(self._cw_url, 'Several texts in one post were found !', self.msg_func)
                
                if not 'author' in par:
                    self._cw_add_to_result_noncritical_error(const.ERROR_POST_AUTHOR_NOT_FOUND, self._cw_get_post_repr(post_id = par['post_id']))
                    par['author'] = ''
                elif par['author'] == '':
                    self._cw_add_to_result_noncritical_error(const.ERROR_POST_AUTHOR_EMPTY, self._cw_get_post_repr(post_id = par['post_id']))
                if not 'date' in par:
                    self._cw_add_to_result_noncritical_error(const.ERROR_POST_DATE_NOT_FOUND, self._cw_get_post_repr(post_id = par['post_id']))
                    par['date'] = ''
                elif par['date'] == '':
                    self._cw_add_to_result_noncritical_error(const.ERROR_POST_DATE_EMPTY, self._cw_get_post_repr(post_id = par['post_id']))

                _dt_in_str, _dt_in_dt = self._str_to_date.get_date(par['date'], type_res = 'S,D')
                
                #if self._cw_check_date_deep(_dt_in_dt) or par['post_id'] in self._cw_activity['post_list']:
                if self._cw_check_date_deep(_dt_in_dt) or self._sn_recrawler_checker.is_crawled_post(_dt_in_dt):
                    self._stop_post_scraping = not par['is_fixed_post'] #non stop for fixed posts
                    return None, par

                self._cw_post_repl_list.append(par['post_id'])

                for res in result:
                    self._cw_scrape_result.append(
                        {
                        'result_type': 'POST',
                        'url': self._cw_url,
                        'sn_id': self._cw_group_id,
                        'sn_post_id': par['post_id'],
                        'sn_post_parent_id': 0,
                        'author': par['author'],
                        'content_date': _dt_in_str,
                        'content_header': '',
                        'content': crawler.remove_empty_symbols(res.text)
                        }
                    )
                    if self._cw_debug_mode:
                        self.msg('POST. Group ID = '+self._cw_group_id+'   Post ID = '+par['post_id']+'    _cw_post_counter = '+str(self._cw_post_counter)+'    _cw_fetch_post_counter = '+str(self._cw_fetch_post_counter))
                        self.msg(' url: '+self._cw_url)
                        self.msg(' Author: '+par['author']+'    Date: '+self._str_to_date.get_date(par['date']))
                        self.msg(crawler.remove_empty_symbols(res.text))
                        self.msg('\n____________________________________________________________\n')
                    
                    self._cw_fix_last_date(par['post_id'], _dt_in_dt)
                        
            else:
                _descr = 'Text not found ! \n '+self._cw_url
                self.warning(_descr)
                self._cw_add_to_result_warning(_descr, {})
        
        return None, par
    
    def _cw_scrap_replies(self, result, par, **kwargs):
        if result == None: return None, par

        if kwargs['mode'] == 'repl dived':
            return result, par
        
        elif kwargs['mode'] == 'author':
            par['author'] = crawler.remove_empty_symbols(result.text)
            par['author_id'] = result.attrs['data-from-id']

        elif kwargs['mode'] == 'date':
            par['date'] = crawler.remove_empty_symbols(result.text)

        elif kwargs['mode'] == 'repl text':

            _parent_id = par['parent_id'] if kwargs['deep_parent'] == '' else kwargs['deep_parent']

            if not 'author' in par:
                self._cw_add_to_result_noncritical_error(const.ERROR_REPLY_AUTHOR_NOT_FOUND, self._cw_get_post_repr(post_id = par['post_id'], repl_id = par['reply_id'], parent_id =_parent_id ))
                par['author'] = ''
            elif par['author'] == '':
                self._cw_add_to_result_noncritical_error(const.ERROR_REPLY_AUTHOR_EMPTY, self._cw_get_post_repr(post_id = par['post_id'], repl_id = par['reply_id'], parent_id =_parent_id ))
            if not 'date' in par:
                self._cw_add_to_result_noncritical_error(const.ERROR_REPLY_DATE_NOT_FOUND, self._cw_get_post_repr(post_id = par['post_id'], repl_id = par['reply_id'], parent_id =_parent_id ))
                par['date'] = ''
            elif par['date'] == '':
                self._cw_add_to_result_noncritical_error(const.ERROR_REPLY_DATE_EMPTY, self._cw_get_post_repr(post_id = par['post_id'], repl_id = par['reply_id'], parent_id =_parent_id ))

            _dt_in_str, _dt_in_dt = self._str_to_date.get_date(par['date'], type_res = 'S,D')

            if par['post_id'] == _parent_id:
                _type_reply = 'REPLY'
            else:
                _type_reply = 'REPLY to REPLY'

            if self._cw_debug_mode: 
                if self._sn_recrawler_checker.is_reply_out_of_upd_date(par['post_id'], _dt_in_dt):
                    self.msg('SKIPPED:')
                self.msg(_type_reply + ' Group ID = '+self._cw_group_id + ' Post ID = '+par['post_id']+'  Reply ID = '+par['reply_id']+'  Parent ID = '+_parent_id+' url: '+self._cw_url)
                self.msg(' Author: '+par['author']+'    author_id: '+par['author_id']+'    Date: '+self._str_to_date.get_date(par['date']))
                self.msg(' _cw_date_deep '+str(self._cw_date_deep)+
                        '    reply wait-date: '+str(self._sn_recrawler_checker.get_post_out_of_date(par['post_id'],'wait_date'))+
                        '    reply upd-date: '+str(self._sn_recrawler_checker.get_post_out_of_date(par['post_id'],'upd_date'))
                        )
                self.msg(crawler.remove_empty_symbols(result.text))
                self.msg('\n       --.....----------------------------------.....-----------\n')

            if self._cw_check_date_deep(_dt_in_dt):
                if _type_reply == 'REPLY':
                    self._stop_repl_scraping = True
                else:
                    self._stop_repl2_scraping = True
                if self._cw_debug_mode:
                    self.msg('-- SET '+_type_reply+' STOP SCRAPING -- SKIPPED -- by _cw_check_date_deep')
                return None, par

            if self._sn_recrawler_checker.is_reply_out_of_wait_date(par['post_id'], _dt_in_dt):
                if _type_reply == 'REPLY':
                    self._stop_repl_scraping = True
                else:
                    self._stop_repl2_scraping = True
                if self._cw_debug_mode:
                    self.msg('-- SET '+_type_reply+' STOP SCRAPING -- SKIPPED -- by is_reply_out_of_wait_date')
                return None, par

            res_unit = { 
                'result_type': _type_reply,
                'url': self._cw_url,
                'sn_id': self._cw_group_id,
                'sn_post_id': par['reply_id'], 
                'sn_post_parent_id': _parent_id,
                'author': par['author'],
                'content_date': _dt_in_str,
                'content_header': '',
                'content': crawler.remove_empty_symbols(result.text)
                }

            if not self._sn_recrawler_checker.is_reply_out_of_upd_date(par['post_id'], _dt_in_dt):
                self._cw_scrape_result.append(res_unit)

            self._cw_fix_last_date(par['post_id'], _dt_in_dt)
            
        return None, par

    def _cw_scrap_repl_show_next(self, result, par, **kwargs):
        if result == None: return None, par
        
        if kwargs['mode'] == 'wrap deep':
            return result, par

        elif kwargs['mode'] == 'show next':
            if result.text != 'Показать предыдущие комментарии': 
                return None, par

            #z = self._cw_get_post_id(result.attrs['onclick'], 'Reply id for show prev repl list not found !')
            z = self._cw_get_post_id(result.attrs['href'], 'Reply id for show prev repl list not found !', mode = 'href')

            self._cw_post_repl2_list.append(
                {'item_id': z['parent_id'], #z['post_id'], #id parent reply
                 'count': result.attrs['data-count'],
                 'offset': result.attrs['data-offset'],
                 'post_id': par['post_id'] #id post
                 }
                )

            return result, par

        return result, par

    def _cw_scrap_subscribers(self, result, par, **kwargs):
        self.msg(str(result))

        if result == None: return None, par
        
        if kwargs['mode'] == '-':
            return result, par

        elif kwargs['mode'] == 'number':

            try:
                self._cw_num_subscribers = int(result.text.replace(' ', ''))
                self._cw_scrape_result.append( {'result_type': const.CW_RESULT_TYPE_NUM_SUBSCRIBERS, 'sn_id': self._cw_group_id,  'num_subscribers': self._cw_num_subscribers } )  
            except:
                self._cw_add_to_result_noncritical_error(const.ERROR_SCRAP_NUMBER_SUBSCRIBERS, self._cw_get_post_repr())

            return result, par

        return result, par

    def _cw_add_to_result_noncritical_error(self, err_type, description):
        self._cw_scrape_result.append({
            'result_type': const.CW_RESULT_TYPE_ERROR,
            'err_type': err_type,
            'err_description': description,
            'datetime': date.date_to_str(datetime.datetime.now())
            })
        
        if not err_type in self._cw_noncritical_error_counter:
           self._cw_noncritical_error_counter[err_type] = 0
        
        self._cw_noncritical_error_counter[err_type] += 1

    def _cw_add_to_result_critical_error(self, err_type, description, stop_process = False):
        self._cw_scrape_result.append({
            'result_type': const.CW_RESULT_TYPE_CRITICAL_ERROR,
            'err_type': err_type,
            'err_description': description,
            'datetime': date.date_to_str(datetime.datetime.now()),
            'stop_process': stop_process
            })

    def _cw_add_to_result_warning(self, description, ext_dict):
        self._cw_scrape_result.append(
            {
             **{
                'result_type': const.CW_RESULT_TYPE_WARNING,
                'event_description': description,
                'datetime': date.date_to_str(datetime.datetime.now())
                }, 
             **ext_dict 
            } )

    def _cw_get_post_repr(self, post_id = '', repl_id = '', parent_id = ''):
        _repr = ''
        _repr = _repr + 'url: '+self._cw_url + '\n'
        _repr = _repr + 'group_id: '+self._cw_group_id + '\n'
        if post_id != '': 
            _repr = _repr + 'post_id: '+post_id + '\n'
        if repl_id != '': 
            _repr = _repr + 'repl_id: '+repl_id + '\n'
        if parent_id != '': 
            _repr = _repr + 'parent_id: '+parent_id + '\n'
        
        return _repr

    def _cw_fix_last_date(self, post_id, dt):
        if dt == None or dt == const.EMPTY_DATE: return

        if not post_id in self._cw_last_dates_activity:     #first
            self._cw_last_dates_activity[post_id] = dt
        elif self._cw_last_dates_activity[post_id] < dt:    #fix the latest datetime
            self._cw_last_dates_activity[post_id] = dt

        if self._cw_group_last_date_activity < dt:
            self._cw_group_last_date_activity = dt

    def _cw_trans_last_dates_to_scrape_result(self):
        for post_id in self._cw_last_dates_activity:
            self._cw_scrape_result.append({
                'result_type': const.CW_RESULT_TYPE_DT_POST_ACTIVITY,
                'post_id': post_id,
                'dt': date.date_to_str(self._cw_last_dates_activity[post_id])
                })
        
        self._cw_last_dates_activity = {} #after transfer to result clear dict

        #fix last dt activity for group
        if self._cw_group_prev_last_date_activity < self._cw_group_last_date_activity:
            self._cw_group_prev_last_date_activity = self._cw_group_last_date_activity
            self._cw_scrape_result.append({
                'result_type': const.CW_RESULT_TYPE_DT_GROUP_ACTIVITY,
                'dt': date.date_to_str(self._cw_group_last_date_activity)
                })

    def _cw_check_date_deep(self, dt):
        if self._cw_date_deep == const.EMPTY_DATE or dt == const.EMPTY_DATE:
            return False
        return dt < self._cw_date_deep
    
    def _scrape_result_empty(self):
        if len(self._cw_scrape_result) == 0:
            return True
        if len(self._cw_scrape_result) == 1:
            if self._cw_scrape_result[0]['result_type'] == const.CW_RESULT_TYPE_HTML:
                self._cw_scrape_result.clear()
                return True
        return False

    def _is_good_status_code(self, status_code, get_url, stop_if_broken = False):
        if status_code == const.HTTP_STATUS_CODE_200:
            return True
        else:
            _descr = 'Status code received: '+str(status_code)
            _descr += '\n'+get_url
            if stop_if_broken:
                ext_dict = { 'wall_processed': True }
            else:
                ext_dict = {}
            self._cw_add_to_result_warning(_descr, ext_dict)
            return False

    def _check_user_interrupt(self):
        if self._cw_need_stop_checker == None:
            return False
        self._cw_need_stop_checker.need_stop()


    def _write_debug_file(self, data):
        if self._write_file_func == None:
            self.msg(str(data))
        else:
            self._write_file_func(str(data))
