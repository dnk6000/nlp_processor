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
import exceptions

import sys

from html.parser import HTMLParser  

from bs4 import BeautifulSoup

import time

#import asyncio
#from pyppeteer import launch


class social_net_crawler():

    def __init__(self, 
                 base_search_words = None, 
                 msg_func = None, 
                 add_db_func = None, 
                 crawl_method = 'api'):
        """
        crawl_method = 'api' / 'browse'
        """

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

        self.msg_func = msg_func        # функция для сообщений
        self.add_db_func = add_db_func  # функция для добавления в БД

        self.api_request_pause_sec = 1  # пауза между запросами к API
        self.api_limit_res = 1000       # максимум записей возвращаемых API

        self.id_cash = []

    def msg(self, message):

        if not self.msg_func == None:
            try:
                self.msg_func(message)
            except:
                pass

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

        


class crawler_vk(social_net_crawler):

    def __init__(self, 
                 login = '', 
                 password = '', 
                 base_search_words = None, 
                 msg_func = None, 
                 add_db_func = None
                 ):

        super().__init__(base_search_words, msg_func, add_db_func)

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

        self._crawl_wall_define_tags()

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

    def _scrape_scroll_page_searching_groups(self, htmltxt):
        
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
            
            self._scrape_scroll_page_searching_groups(_htmltxt)

            _search_group_offset += 20

    def _add_groups_to_db(self, groups_list):

        n = len(groups_list)
        c = 0
        self.msg('Add groups to DB: ' + str(n))

        for Gr in groups_list:
            c += 1
            self.msg('Add groups to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(Gr['id']) + ' ' + Gr['name'])
            self.add_db_func('vk',
                             'group',
                             Gr['id'],
                             Gr['name'],
                             Gr['screen_name'],
                             Gr['is_closed'] == 1
            )


    ##########################
    # wall crawling procedures
    ##########################
    def _crawl_wall_define_tags(self):

        cts = CrawlTagStructure
        NoneFunc = None
        NoneFuncParam = None
        OneTag = False
        MultiTag = True

        nPostTag      = { 'class' : re.compile('^_post post.*') } 

        self._wall_tags_NotFound = [ [ cts( { 'class' : re.compile('message_page_title') } , OneTag , self._crawl_wall_check_not_found , 'tag 1'),
                                       cts( { 'class' : re.compile('message_page_body') }  , OneTag , self._crawl_wall_check_not_found , 'tag 2')
                                     ]
                                   ]

        self._wall_tags_FixedArea = [ [ cts( { 'class' : re.compile('wall_fixed') } , OneTag  , self._crawl_wall_scrap_fixed_area , 'all fixed area') ],
                                      [ cts( nPostTag                               , MultiTag, self._crawl_wall_scrap_fixed_area , 'posts in fixed area') ]
                                    ]

        self._wall_tags_Post = [ [ cts( nPostTag                               , MultiTag, NoneFunc, NoneFuncParam) ],
                                 [ cts( { 'class' : re.compile('wall_text') }  , MultiTag, NoneFunc, NoneFuncParam) ]
                               ]

        self._wall_tags_Replies = [ [ cts( { 'class' : re.compile('^replies_list') }      , OneTag  , NoneFunc, NoneFuncParam) ],
                                    [ cts( { 'class' : re.compile('^reply reply_dived') } , MultiTag, NoneFunc, NoneFuncParam) ],
                                    [ cts( { 'class' : re.compile('^wall_reply_text') }   , OneTag  , NoneFunc, NoneFuncParam) ]
                                  ]


    def crawl_wall(self, group_id):

        self._crawl_wall_not_found_error = 0

        self._crawl_wall_session = requests_html.HTMLSession()

        self._crawl_wall_url = r'https://vk.com/' + 'club' + str(group_id)
        try:
            data = self._crawl_wall_session.get(self._crawl_wall_url, headers = self.headers)
        except Exception as e:
            raise exceptions.CrawlVkByBrowserError(self._crawl_wall_url, 'Error when trying to load the wall !', self.msg_func)

        self._crawl_wall_soup = BeautifulSoup(data.text, "html.parser")

        #Сообщество не найдено Error
        self._crawl_wall_scraping_by_cts(self._wall_tags_NotFound)
        if self._crawl_wall_not_found_error == 2:
            return (-1, 'Not found')

        self._crawl_wall_scraping_by_cts(self._wall_tags_FixedArea)


        self._crawl_wall_soup = None #clean up before exiting
        
        return (0, 'Sucsess')


    def _crawl_wall_scraping_by_cts(self, cts_list, first_step = True):
        """recurrently scrape self.wall_soup by gived cts_list (elements of which = CrawlTagStructure)
        """

        if len(cts_list) == 0:  return
        
        no_found = False

        if first_step:  local_soup = self._crawl_wall_soup
        else:           local_soup = self.wall_found_tags

        for cts in cts_list.pop(0):
            if first_step:
                if cts.multi:   self.wall_found_tags = self._crawl_wall_soup.findAll('', cts.tag_name )
                else:           self.wall_found_tags = self._crawl_wall_soup.find('', cts.tag_name )
            else:
                if cts.multi:   self.wall_found_tags = self.wall_found_tags .findAll('', cts.tag_name )
                else:           self.wall_found_tags = self.wall_found_tags .find('', cts.tag_name )

            if cts.func != None:
                if cts.func_par == None:    cts.func()
                else:                       cts.func(cts.func_par)
            
            if cts.multi:   no_found = len(self.wall_found_tags) == 0
            else:           no_found = self.wall_found_tags == None

        if no_found: return

        self._crawl_wall_scraping_by_cts(cts_list, first_step = False)


    def _crawl_wall_scrap_fixed_area(self, mode):
        if mode == 'all fixed area':
            if self.wall_found_tags == None:
                #no fixed tags
                pass

        else: #mode == 'posts in fixed area'
            if len(self.wall_found_tags) > 0:
                if len(self.wall_found_tags) > 1:
                    raise exceptions.CrawlVkByBrowserError(self._crawl_wall_url, 'Several fixes were found !', self.msg_func)

                match = re.match( r'^post(-\d*)_(\d*)', self.wall_found_tags[0].attrs['id'] ) 

                if match:
                    self._crawl_wall_fixed_post_num = match.group(2)
                else:
                    raise exceptions.CrawlVkByBrowserError(self._crawl_wall_url, 'Fixed post item_id not found !', self.msg_func)

    def _crawl_wall_check_not_found(self, mode):
        if self.wall_found_tags == None: return

        if mode == 'tag 1':
            if 'Ошибка' in self.wall_found_tags.text:
                self._crawl_wall_not_found_error += 1
        if mode == 'tag 2':
            if 'Сообщество не найдено' in self.wall_found_tags.text:
                self._crawl_wall_not_found_error += 1

    def _crawl_wall_post_comment(self):

        pass

class CrawlTagStructure():
    def __init__(self, tag_name, multi = False, func = None, func_par = None):
        
        self.tag_name = tag_name
        self.multi = multi
        self.func = func
        self.func_par = func_par

    def __repr__(self):
        return str(self.__dict__)

class TagNode():
    def __init__(self, 
                 tag_name, 
                 multi = False, 
                 process_func = None, 
                 result_func = None, 
                 result_func_par = None):
        """ tag_name -  regular expression with tag name
            multi    -  True - find tag multi times, False - once
            process_func - function to process 'incoming parameters' in class function Scan
            result_func  - function to process result of process_func
            result_func_par - any parameters for result_func
        """
        self.tag_name = tag_name
        self.multi = multi  
        self.process_func = func
        self.result_func = result_func
        self.result_func_par = result_func_par

    def __repr__(self):
        return str(self.__dict__)

class TagTree():
    def __init__(self, node = None):
        """the type of node type must be 'list', 'TagNode' or None (default)
        """
        
        if type(node) == list:
            self.nodes = node
        elif type(node) == TagNode:
            self.nodes = [_node]
        else:
            self.nodes = list()
        
        self.childs = list()
        self.parent = None

    def __repr__(self):
        return str(self.__dict__)

    def add(self, childs):
        """childs type must be 'list', 'TagNode' or 'TagTree'
        """

        if type(childs) == list:
            for child in childs:
                if type(child) == TagTree:
                    self.childs.append(child)
                if type(child) == TagNode:
                    self.childs.append(TagTree(child))
                else:
                    raise 'Incorrect type of child in child-list for TagTree class!'
        elif type(childs) == TagTree:
            self.childs.append(childs)
        elif type(childs) == TagNode:
            self.childs.append(TagTree(childs))
        else:
            raise 'Incorrect type of var ''childs'' for TagTree class!'

    def scan(self, income_par):
        
        result_of_process_func = None 

        for node in self.nodes:
            if node.process_func != None:
                result_of_process_func = node.process_func(income_par)
            if node.result_func != None:
                result_of_result_func = node.result_func(result_of_process_func, node.result_func_par)
            else:
                result_of_result_func = result_of_process_func

        for child in self.childs:
            child.scan(result_of_result_func)




def get_psw_mtyurin():
    
    with open('C:\Temp\mypswvk.txt', 'r') as f:
        psw = f.read()

    return 'Bey'+psw+'00'



if __name__ == "__main__":

    Crawler = crawler_vk(msg_func = print)
    res = Crawler.crawl_wall(777716758516)
    print(res)
    #try:
    #    res = Crawler.crawl_wall(777716758516)
    #    print(res)
    #except:
    #    pass
    sys.exit(0)

    CassDB = pgree.CassandraDB(password=pgree.get_psw_mtyurin())
    CassDB.Connect()

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


