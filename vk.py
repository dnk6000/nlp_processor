import vk_requests
import vk_requests.exceptions

import requests
import requests_html
import lxml.html

import re

from datetime import datetime
from itertools import combinations_with_replacement
from itertools import combinations
from itertools import product
from time import sleep

import pgree
import crawler
import scraper
import exceptions

import sys

from html.parser import HTMLParser  

from bs4 import BeautifulSoup

import time

#import asyncio
#from pyppeteer import launch


class CrawlerSocialNet:

    def __init__(self, 
                 login = '', 
                 password = '', 
                 base_search_words = None, 
                 msg_func = None,
                 warning_func = None,
                 add_db_func = None, 
                 id_project = 0,
                 crawl_method = 'api'
                 ):
        """
        crawl_method = 'api' / 'browse'
        """

        self.id_project = id_project

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
            self.msg_func(message)

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

        while len(self.search_list) > 0:
            search_elem = self.search_list.pop(0)
            self._crawl_groups(search_elem)
            
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
        
        self._add_groups_to_db(res['groups_list'])

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

        
class CrawlerVk(CrawlerSocialNet):
    def iiiii(self, 
                 login = '', 
                 password = '', 
                 base_search_words = None, 
                 msg_func = None, 
                 add_db_func = None,
                 id_project = 0
                 ):

        super().__init__(base_search_words = base_search_words, 
                         msg_func = msg_func, 
                         add_db_func = add_db_func, 
                         id_project = id_project)

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

        self.login = login
        self.password = password
        
        self.service_token = ''

        self.api_available = self.login != ''

        self._cw_define_tags()

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
        #    print(e)
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
        #print('onLoginDone' in response.text)


        token_params = {'client_id': '7467601', 
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
    def __init__(self, 
                 login = '', 
                 password = '', 
                 base_search_words = None, 
                 msg_func = None, 
                 add_db_func = None,
                 id_project = 0
                 ):

        super().__init__(base_search_words = base_search_words, 
                         msg_func = msg_func, 
                         add_db_func = add_db_func, 
                         id_project = id_project)

    def _crawl_groups_api(self, search_elem):

        sleep(self.api_request_pause_sec)

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
        tagsArticleBody     = soup.findAll('', { 'class' : re.compile(articleBodyTag) })
        for tagBody in tagsArticleBody:
            
            self.count += 1
            #print('group id '+tagBody.attrs['data-id'])

            tagBlocks = tagBody.findAll(re.compile('^img'), { 'class' : re.compile('search_item_img') })
            for tagBlock in tagBlocks:
                print(str(self.count) + ' group id: '+tagBody.attrs['data-id'] + '     group name: '+tagBlock.attrs['alt'])
                groups_list.append({ 'id'   : tagBody.attrs['data-id'], 
                                     'name' : tagBlock.attrs['alt'],
                                     'screen_name' : '',
                                     'is_closed' : 0
                                   })

        self._add_groups_to_db(groups_list)

    def _scrape_fetch_page_searching_groups(self, htmltxt):
        
        groups_list = list()

        soup = BeautifulSoup(htmltxt, "html.parser")

        articleBodyTag     = 'groups_row search_row clear_fix'
        tagsArticleBody     = soup.findAll('', { 'class' : re.compile(articleBodyTag) })
        for tagBody in tagsArticleBody:
            
            self.count += 1
            #print('group id '+tagBody.attrs['data-id'])

            tagBlocks = tagBody.findAll(re.compile('^img'), { 'class' : re.compile('search_item_img') })
            for tagBlock in tagBlocks:
                print(str(self.count) + ' group id: '+tagBody.attrs['data-id'] + '     group name: '+tagBlock.attrs['alt'])
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

        n = len(groups_list)
        c = 0
        self.msg('Add groups to DB: ' + str(n))

        for Gr in groups_list:
            c += 1
            self.msg('Add groups to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(Gr['id']) + ' ' + Gr['name'])
            self.add_db_func(self.id_project,
                             'vk',
                             'group',
                             Gr['id'],
                             Gr['name'],
                             Gr['screen_name'],
                             Gr['is_closed'] == 1
            )


class CrawlerVkWall(CrawlerVk):
    def __init__(self, 
                 login = '', 
                 password = '', 
                 base_search_words = None, 
                 msg_func = None, 
                 add_db_func = None,
                 id_project = 0
                 ):

        super().__init__(base_search_words = base_search_words, 
                         msg_func = msg_func, 
                         add_db_func = add_db_func, 
                         id_project = id_project)

    
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
        fnpl = self._cw_find_tags_post_list
        fnrpl = self._cw_find_tags_repl_list
        pr = lambda p1, p2, p3: { 'tag_key':p1, 'multi': p2, 'mode': p3, 'deep_parent': '' }

        nPostTag = rc('^_post post.*')
        nDateTag = rc('^rel_date')
        nAuthorTag = rc('^author')
        
        self._cw_tg_NotFound = TT ( [ TN( fn, self._cw_check_not_found , pr(rc('message_page_title'), OneTag, 'tag 1') ),
                                      TN( fn, self._cw_check_not_found , pr(rc('message_page_body') , OneTag, 'tag 2') )
                                    ]
                                  )

        self._cw_tg_Subscribers = TT ( TN( fn, None , pr( { 'aria-label' : 'Подписчики' }, OneTag  , '-' ) ) )
        self._cw_tg_Subscribers.add  (    TN( fn, self._cw_scrap_subscribers , pr( { 'class' : 'header_count fl_l' }, OneTag, 'number') ) )

        self._cw_tg_FixedArea = TT ( TN( fn, self._cw_scrap_fixed_area , pr(rc('wall_fixed'), OneTag  , 'all fixed area'     ) ) )
        self._cw_tg_FixedArea.add  (    TN( fn, self._cw_scrap_fixed_area , pr(nPostTag        , MultiTag, 'posts in fixed area') ) )
        
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
        self._cw_tg_ShowPrevRepl.add  (    TN( fn, self._cw_scrap_repl_show_next , pr(ro('^return wall.showNextReplies')   , OneTag  , 'show next') ) )


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

        self._str_to_date = scraper.StrToDate(['dd mmm в hh:mm', 'dd mmm yyyy'], url = self.url, msg_func = self.msg_func)

    def crawl_wall(self, group_id): # _cw_

        self._cw_debug_mode = True
        self._cw_debug_post_filter = '113608'
        #self._cw_debug_post_filter = ''
        if self._cw_debug_mode: self._cw_num_posts_request = 100

        self._cw_num_subscribers = 0
        self._cw_fixed_post_id = ''
        self._cw_group_id = ''
        self._cw_post_counter = 0
        self._cw_post_counter2 = 0
        self._cw_post_repl_list = []  #first level replies
        self._cw_post_repl2_list = [] #second level replies = href 'Показать предыдущие комментарии'

        self._cw_num_posts_request = 10  #number of posts per one fetch-request
        self._cw_num_repl_request = 20  #number of replies received per request

        self._cw_session = requests_html.HTMLSession()

        if type(group_id) == str:
            self._cw_url = self.url + 'wall-' + group_id
        else:
            self._cw_group_id = str(group_id)
            self._cw_url = self.url + 'club' + str(group_id)

        self._cw_url_fetch = self.url + 'al_wall.php'

        try:
            d = self._cw_session.get(self._cw_url, headers = self.headers)
        except Exception as e:
            raise exceptions.CrawlVkByBrowserError(self._cw_url, 'Error when trying to load the wall !', self.msg_func)

        self._cw_soup = BeautifulSoup(d.text, "html.parser")

        #is group found ?
        self._cw_signs_count = 0
        self._cw_tg_NotFound.scan(self._cw_soup)
        if self._cw_signs_count == 2:
            return (-1, 'Not found')

        #get subscribers
        self._cw_tg_Subscribers.scan(self._cw_soup)
        self._cw_update_num_subscribers()

        #get fixed posts
        self._cw_tg_FixedArea.scan(self._cw_soup)

        _fetch_enable = True

        while _fetch_enable:
            self._cw_fetch_post_counter = 0

            #get posts
            self._cw_tg_Posts.scan(self._cw_soup)

            self._cw_get_post_replies()
            self._cw_get_post_replies2() #second level

            #fetch browser page
            if self._cw_fetch_post_counter >= self._cw_num_posts_request:  #задать в константе
                self._cw_fetch_wall()
            else:
                _fetch_enable = False

        return (0, 'Sucsess')


    def _cw_get_post_replies(self):
        '''request replies for the posts from '_cw_post_repl_list' and scrape results 
        '''
        while len(self._cw_post_repl_list) > 0:
            _post_id = self._cw_post_repl_list.pop(0)
            _first_step = True
            _replies_offset = 0
            _fetch_count = 0
            self._cw_fetch_repl_counter = 0

            while self._cw_fetch_repl_counter >= self._cw_num_repl_request or _first_step:
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

                self._cw_fetch(par_data, 'Error when trying to fetch post replies ! '+str(par_data))

                self._cw_fetch_repl_counter = 0
                self._cw_tg_Replies.scan(self._cw_soup)

                self._cw_tg_ShowPrevRepl.scan(self._cw_soup)  #addition ShowPrev-list

                _fetch_count += 1

                if self._cw_debug_mode:
                    print('############################################################')
                    print('REPLY SCROLL.  _post_id = '+_post_id+' _cw_fetch_repl_counter = '+str(self._cw_fetch_repl_counter)+' _fetch_count = '+str(_fetch_count))
                    print(par_data)
                    print('############################################################')

    def _cw_get_post_replies2(self):
        '''press to href 'Показать следующие комментарии' and scrape request results 
        '''
        while len(self._cw_post_repl2_list) > 0:
            _list_elem = self._cw_post_repl2_list.pop(0)
            _first_step = True
            _replies_offset = int(_list_elem['offset'])
            _fetch_count = 0
            self._cw_fetch_repl_counter = 0

            while self._cw_fetch_repl_counter >= self._cw_num_repl_request or _first_step:
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

                self._cw_fetch(par_data, 'Error when trying to fetch post replies ! '+str(par_data))

                self._cw_fetch_repl_counter = 0
                self._cw_tg_Replies.set_par('deep_parent', _list_elem['item_id'])
                self._cw_tg_Replies.scan(self._cw_soup)

                _fetch_count += 1

                if self._cw_debug_mode:
                    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                    print('REPLY to REPLY SCROLL.  _post_id = '+_list_elem['item_id']+' _cw_fetch_repl_counter = '+str(self._cw_fetch_repl_counter)+' _fetch_count = '+str(_fetch_count))
                    print(par_data)
                    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

    def _cw_fetch(self, par_data, err_txt = ''):  #fetch !!!!!!!!!!!!
        '''make post-request with par_data params
        '''
        time.sleep(2)   #!!!!!!!!!! smart func needed !
        try:
            d = self._cw_session.post(self._cw_url_fetch, headers = self.headers, data = par_data)
        except Exception as e:
            raise exceptions.CrawlVkByBrowserError(self._cw_url, err_txt, self.msg_func)
            
        txt = d.text.replace('\\', '')
        txt = txt.replace('<!--{"payload":[0,["', '<')
        if txt[0] != '>':
            txt = '>' + txt

        #print(txt)

        self._cw_soup = BeautifulSoup(txt, "html.parser")

    def _cw_fetch_wall(self):
        self._cw_wall_fetch_par['fixed'] = self._cw_fixed_post_id
        self._cw_wall_fetch_par['owner_id'] = '-'+self._cw_group_id
        self._cw_wall_fetch_par['offset'] = str(self._cw_post_counter)
        self._cw_wall_fetch_par['wall_start_from'] = str(self._cw_post_counter2)

        self._cw_fetch(self._cw_wall_fetch_par, 'Error when trying to fetch the wall !')


    def _cw_get_post_id(self, html_attr, error_msg = ''):
        if 'showNextReplies' in html_attr:
            match = re.match( r'(.*)-(\d*)_(\d*)', html_attr ) 
        else:
            match = re.match( r'^(post|replies|\/wall)-(\d*)_(\d*)', html_attr ) 

        if match:
            return {'group_id':match.group(2), 'post_id': match.group(3) }
        else:
            raise exceptions.CrawlVkByBrowserError(self._cw_url, error_msg, self.msg_func)



    def _cw_find_tags(self, soup, par, **kwargs):
        if soup == None: return None, par

        if kwargs['multi']:
            res = soup.findAll('', kwargs['tag_key'])
        else:
            res = soup.find('', kwargs['tag_key'])

        return res, par

    def _cw_find_tags_post_list(self, soup, par, **kwargs):
        if soup == None: return None, par

        z = self._cw_get_post_id(soup.attrs['id'], 'Post item_id not found !')

        par['post_id'] = z['post_id']

        #Debug
        if self._cw_debug_mode and self._cw_debug_post_filter != '':
            if par['post_id'] != self._cw_debug_post_filter: return None, par

        self._cw_post_repl_list.append(par['post_id'])

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
        if result == None: return None

        if kwargs['mode'] == 'all fixed area':
            return result, par

        else: #mode == 'posts in fixed area'
            if len(result) > 0:
                if len(result) > 1:
                    raise exceptions.CrawlVkByBrowserError(self._cw_url, 'Several fixed posts were found !', self.msg_func)

                z = self._cw_get_post_id(result[0].attrs['id'], 'Fixed post item_id not found !')

                self._cw_fixed_post_id = z['post_id']

                self._cw_post_counter -= 1

        return None, par

    def _cw_check_not_found(self, result, par, **kwargs):

        if result == None: return None, par

        if kwargs['mode'] == 'tag 1':
            if 'Ошибка' in result.text:
                self._cw_signs_count += 1
        if kwargs['mode'] == 'tag 2':
            if 'Сообщество не найдено' in result.text:
                self._cw_signs_count += 1

    def _cw_scrap_posts(self, result, par, **kwargs):
        if result == None: return None, par

        if kwargs['mode'] == 'posts list':
            
            if len(result) > 0 and (self._cw_group_id == ''):
                z = self._cw_get_post_id(result[0].attrs['id'], 'Group id not found !')

                self._cw_group_id = z['group_id']

            return result, par

        elif kwargs['mode'] == 'author':
            par['author'] = result.text

        elif kwargs['mode'] == 'date':
            par['date'] = result.text

        elif kwargs['mode'] == 'wall texts':
            if len(result) > 0:
                self._cw_post_counter += 1
                self._cw_post_counter2 += 1
                self._cw_fetch_post_counter += 1

                if len(result) > 1:
                    raise exceptions.CrawlVkByBrowserError(self._cw_url, 'Several texts in one post were found !', self.msg_func)
                
                for iTextTag in result:
                    # вставить запись текста в БД
                    if self._cw_debug_mode:
                        print('POST. Group ID = '+self._cw_group_id+'   Post ID = '+par['post_id']+'    _cw_post_counter = '+str(self._cw_post_counter)+'    _cw_fetch_post_counter = '+str(self._cw_fetch_post_counter))
                        print('Author: '+par['author']+'    Date: '+self._str_to_date.get_date(par['date']))
                        print(crawler.remove_empty_symbols(iTextTag.text))
                        print('\n____________________________________________________________\n')
                        
                        self._cw_add_to_db_data_text(
                                content = crawler.remove_empty_symbols(iTextTag.text), 
                                content_date = self._str_to_date.get_date(par['date']), 
                                sn_id = int(self._cw_group_id), 
                                sn_post_id = int(par['post_id']), 
                                sn_post_parent_id = 0
                                )
            else:
                self.warning('Warning: Text not found ! \n '+self._cw_url)
        
        return None, par
    
    def _cw_scrap_replies(self, result, par, **kwargs):
        if result == None: return None, par

        if kwargs['mode'] == 'repl dived':
            return result, par
        
        elif kwargs['mode'] == 'author':
            par['author'] = result.text
            par['author_id'] = result.attrs['data-from-id']

        elif kwargs['mode'] == 'date':
            par['date'] = result.text

        elif kwargs['mode'] == 'repl text':

            _parent_id = par['parent_id'] if kwargs['deep_parent'] == '' else kwargs['deep_parent']

            if par['post_id'] == _parent_id:
                if self._cw_debug_mode:
                    print('REPLY. Post ID = '+par['post_id']+'  Reply ID = '+par['reply_id']+'  Parent ID = '+_parent_id)
                    print('Author: '+par['author']+'    author_id: '+par['author_id']+'    Date: '+self._str_to_date.get_date(par['date']))
            else:
                if self._cw_debug_mode:
                    print('REPLY to REPLY. Post ID = '+par['post_id']+'  Reply ID = '+par['reply_id']+'  Parent ID = '+_parent_id)
                    print('Author: '+par['author']+'    author_id: '+par['author_id']+'    Date: '+self._str_to_date.get_date(par['date']))
            if self._cw_debug_mode:
                print(crawler.remove_empty_symbols(result.text))
                print('\n       --.....----------------------------------.....-----------\n')
            
        return None, par

    def _cw_scrap_repl_show_next(self, result, par, **kwargs):
        if result == None: return None, par
        
        if kwargs['mode'] == 'wrap deep':
            return result, par

        elif kwargs['mode'] == 'show next':
            if result.text != 'Показать предыдущие комментарии': 
                return None, par

            z = self._cw_get_post_id(result.attrs['onclick'], 'Reply id for show prev repl list not found !')

            self._cw_post_repl2_list.append(
                {'item_id': z['post_id'],
                 'count': result.attrs['data-count'],
                 'offset': result.attrs['data-offset']
                 }
                )

            return result, par

        return result, par

    def _cw_scrap_subscribers(self, result, par, **kwargs):
        if result == None: return None, par
        
        if kwargs['mode'] == '-':
            return result, par

        elif kwargs['mode'] == 'number':

            try:
                self._cw_num_subscribers = int(result.text.replace(' ', ''))
            except:
                raise exceptions.CrawlVkByBrowserError(self._cw_url, 'Error by scrapping number of subscribers !', self.msg_func)

            return result, par

        return result, par

    def _cw_update_num_subscribers(self):
        if self._cw_num_subscribers == 0:
            return
        if not isinstance(self.add_db_func, dict):  
            return
        self.add_db_func['update_num_subscribers'](
            'vk',
            id_project = self.id_project,
            account_id = int(self._cw_group_id),
            number_subscribers = self._cw_num_subscribers)

    def _cw_add_to_db_data_text(self, 
                                content, 
                                content_date, 
                                sn_id, 
                                sn_post_id, 
                                sn_post_parent_id
                                ):
        if not isinstance(self.add_db_func, dict):  
            return
        self.add_db_func['add_to_db_data_text'](
                            url = self._cw_url, 
                            content = content, 
                            gid_data_html = 0, #!!!!!!!!!
                            content_header = '', 
                            content_date = content_date, 
                            id_project = self.id_project, 
                            sn_network = 'vk', 
                            sn_id = 111, 
                            sn_post_id = 222, 
                            sn_post_parent_id = 333)



def get_psw_mtyurin():
    
    with open('C:\Temp\mypswvk.txt', 'r') as f:
        psw = f.read()

    return 'Bey'+psw+'00'



if __name__ == "__main__":

    cass_db = pgree.CassandraDB()
    crawler_vk = CrawlerVk(login = '89273824101', 
                         password = get_psw_mtyurin(), 
                         base_search_words = ['Фурсов'], 
                         msg_func = print, 
                         add_db_func = cass_db.add_to_db_sn_accounts
                         )
            #CassDB = pgree.CassandraDB(password=pgree.get_psw_mtyurin())
            #CassDB.Connect()

            #Crawler = crawler_vk(msg_func = print, 
            #                     add_db_func = { 'update_num_subscribers' : CassDB.update_sn_num_subscribers,
            #                                     'add_to_db_data_text'    : CassDB.add_to_db_data_text
            #                                   }
            #                     )
            #res = Crawler.crawl_wall(16758516)
    #res = Crawler.crawl_wall('16758516_109038')

    #print(res)
    #try:
    #    res = Crawler.crawl_wall(777716758516)
    #    print(res)
    #except:
    #    pass
    sys.exit(0)

    #Crawler = crawler_vk(login = '89273824101', 
    #                     password = get_psw_mtyurin(), 
    #                     base_search_words = ['пенз'], 
    #                     msg_func = print, 
    #                     add_db_func = CassDB.AddToBD_SocialNet
    #                     )
    Crawler = crawler_vk(login = '89273824101', 
                         password = get_psw_mtyurin(), 
                         base_search_words = ['Фурсов'], 
                         msg_func = print, 
                         add_db_func = CassDB.AddToBD_SocialNet
                         )
    Crawler.id_cash = CassDB.SelectGroupsID()

    #Crawler._crawl_groups_browse('пенз')

    Crawler.crawl_groups() #by API

    CassDB.CloseConnection()


