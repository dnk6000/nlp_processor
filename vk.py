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

    def del___gen_base_search_words(self):
        
        for base_word in self.base_search_words:
            yield base_word


    def del___gen_rus_alph_enum(self, sequence_num, level = 1):
        """ level: =длина элемента комбинации
            sequence_num:  номер набора комбинаций =русский и английский наборы
        """

        #all_combinations = combinations_with_replacement(alph_str, level) #с повторением
        all_combinations = combinations(self.alph_str[sequence_num], level)

        for comb in all_combinations:
            #yield ' '.join(comb)
            yield ''.join(comb)

    def del___gen_search_words(self, word, sequence_num = 0):
        
        for alph in self.gen_rus_alph_enum(sequence_num, level = 2):
            if not ' '+alph in word:
                yield word+' '+alph
        


class crawler_vk(social_net_crawler):

    def __init__(self, login, password, base_search_words = None, msg_func = None, add_db_func = None):

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


        self.login = login
        self.password = password
        
        self.api_request_pause_sec = 2

        self.service_token = ''
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

def get_psw_mtyurin():
    
    with open('C:\Temp\mypswvk.txt', 'r') as f:
        psw = f.read()

    return 'Bey'+psw+'00'



def get_vkapi(service_token):
    
    #api = vk_requests.create_api(app_id=123, login='User', password='Password')

    #user token
    api = vk_requests.create_api(service_token)

    #with open('C:\\Temp\\vktoken.txt', 'r') as f:
    #    lservice_token = f.read()
    api = vk_requests.create_api(service_token=lservice_token)
    #service token
    #api = vk_requests.create_api(service_token="9d3933a79d3933a79d3933a74d9d48c1f699d399d3933a7c3875b521811e40677ed1bea")

    return api


def read_wall(api, uid_p):

    uid = api.users.get(user_ids= uid_p)

    print(uid)

    wall = api.wall.get(owner_id = 421797609, count = 2)

    print('Всего сообщений: ', wall['count'])

    for msg in wall['items']:

        print('-------------------------------------------------------------------------------')
        print('Сообщение: msg_id = ', msg['from_id'])
        print('     Дата: '+datetime.utcfromtimestamp(int(msg['date'])).strftime('%Y-%m-%d %H:%M:%S'))
        print('     Просмотров: ', msg.get('views', {'count': 0})['count'])
        print('     Текст: ', msg['text'])

def user_search(api):

    users = api.users.search(q = 'Андрей Фурсов')

    pass

def database_getRegions(api):

    #countries = api.database.getCountries(code = 'RU')
    #Russia id = 1

    #regions = api.database.getRegions(country_id = 1, q = 'пен')
    #Пенз.область id = 1067455

    try:
        cities = api.database.getCities(country_id = 1, region_id = 1067455)
    except vk_requests.exceptions.VkAPIError:
        print('Change token please !!!')

    pass

def groups_search_default(api, pq):

    groups = api.groups.search(q = pq)

    print(pq+' .default. '+' result: '+str(groups['count']))


def groups_search(api, pq, ptype, psort):

    par = { 'q' : pq, 'type' : ptype, 'sort' : psort }
    #par = { 'type' : ptype, 'sort' : psort }

    #groups = api.groups.search(q = pq, type = ptype, sort = psort)
    groups = api.groups.search(**par)

    print(pq+' type: '+ptype+' sort: '+str(psort)+' result: '+str(groups['count']))
 

def crawl_endless_scroll(login, password):

    url = 'https://vk.com/'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language':'ru-ru,ru;q=0.8,en-us;q=0.5,en;q=0.3',
        'Accept-Encoding':'gzip, deflate',
        'Connection':'keep-alive',
        'DNT':'1'
    }
    #session = requests_html.session()
    session = requests_html.HTMLSession()
    data = session.get(url, headers=headers)
    page = lxml.html.fromstring(data.content)

    form = page.forms[0]
    form.fields['email'] = login
    form.fields['pass'] = password

    response = session.post(form.action, data=form.form_values())
    print('onLoginDone' in response.text)

    url = r'https://vk.com/search?c%5Bper_page%5D=40&c%5Bq%5D=пенз&c%5Bsection%5D=communities'
    data = session.get(url, headers=headers)

    match = re.search(r'"query_id":"\d+"', data.text) 

    if match:
        qid = data.text[match.regs[0][0]+12:match.regs[0][1]-1]

    data.html.render()

    time.sleep(1)

    #url = r'https://sun1-98.userapi.com/c837723/v837723961/4136/kiF3y8paj3o.jpg?ava=1'
    #data2 = session.get(url, headers=headers)

    param_dict = {'act': 'show_more', 
                  'al': '1',
                  'al_ad': '0',
                  'c[q]': 'пенз',
                  'c[section]': 'communities',
                  'offset': '40',
                  'query_id': qid,
                  'real_offset': '40'
                 }  

    data = session.post(url, headers=headers, data=param_dict)
    #data = session.get(url, headers=headers, data=param_dict)     data.html.find('labeled title', first=True)  data._html.lxml
    
    txt = data.text.replace('\\', '')
    
    search_record_tag = 'groups_row search_row clear_fix'
    first_search_tag = '<div class="' + search_record_tag

    match = re.search(r'<!--.+?'+first_search_tag, txt) 

    if match:
        txt = txt[match.regs[0][1]-len(first_search_tag):]

    f=1

def ScrapGroups():

    count = 0

    #with open(r"C:\Work\Python\CasCrawl37\TextFile1.txt", 'r', encoding='utf-8') as f:
    with open(r"C:\Work\Python\CasCrawl37\TextFile1.txt", 'r') as f:
        htmltxt = f.read()
        soup = BeautifulSoup(htmltxt, "html.parser")

        articleBodyTag     = 'groups_row search_row clear_fix'
        tagsArticleBody     = soup.findAll('', { 'class' : re.compile(articleBodyTag) })
        for tagBody in tagsArticleBody:
            
            count += 1
            #print('group id '+tagBody.attrs['data-id'])

            tagBlocks = tagBody.findAll(re.compile('^img'), { 'class' : re.compile('search_item_img') })
            for tagBlock in tagBlocks:
                print(str(count) + 'group id '+tagBody.attrs['data-id'] + '     group name '+tagBlock.attrs['alt'])
    
    with open(r"C:\Work\Python\CasCrawl37\TextFile2.txt", 'r') as f:
        htmltxt = f.read()
        soup = BeautifulSoup(htmltxt, "html.parser")

        articleBodyTag     = 'groups_row search_row clear_fix'
        tagsArticleBody     = soup.findAll('', { 'class' : re.compile(articleBodyTag) })         #soup.findAll('', { 'class' : re.compile('groups_row search_row clear_fix') })
        for tagBody in tagsArticleBody:
            
            count += 1
            #print('group id '+tagBody.attrs['data-id'])

            tagBlocks = tagBody.findAll(re.compile('^img'), { 'class' : re.compile('search_item_img') })
            for tagBlock in tagBlocks:
                print(str(count) + 'group id '+tagBody.attrs['data-id'] + '     group name '+tagBlock.attrs['alt'])

def crawl_endless_scroll_wall():

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language':'ru-ru,ru;q=0.8,en-us;q=0.5,en;q=0.3',
        'Accept-Encoding':'gzip, deflate',
        'Connection':'keep-alive',
        'DNT':'1'
    }
    #session = requests_html.session()
    session = requests_html.HTMLSession()

    url = r'https://vk.com/andrey_fursov'
    data = session.get(url, headers=headers)

    soup = BeautifulSoup(data.text, "html.parser")

    #nPostTag     = { 'id' : re.compile('^post-\d*_\d*'), 'class' : re.compile('^_post.*') }  #'_post_content'
    nPostTag     = { 'class' : re.compile('^_post post.*') } 
    nTextTag     = { 'class' : re.compile('wall_text') } 

    nRepliesTag      = { 'class' : re.compile('^replies_list') }
    nRepliesContTag  = { 'class' : re.compile('^reply reply_dived') }
    nRepliesTextTag  = { 'class' : re.compile('^wall_reply_text') }

    #replies
    #replies_list
    #reply reply_dived
    #reply_text


    nShowNextRepliesTag  = { 'onclick' : re.compile('^return wall.showNextReplies') }

    nShowDeepRepliesTag  = { 'onclick' : re.compile('^return wall.openDeepShortReplies') }


    #tPostTag     = soup.findAll('', { 'class' : nPostTag })
    tPostTag     = soup.findAll('', nPostTag )

    for iPostTag in tPostTag:

        #if not 'Греф' in iPostTag.text:
        if not 'К происходящему в Беларуси. Протесты в Беларуси. В чём там дело?' in iPostTag.text:
            continue

        match = re.match( r'^post(-\d*)_(\d*)', iPostTag.attrs['id'] ) 

        if match:
            _replies_owner_id = match.group(1)
            _replies_item_id = match.group(2)

        else:
            _replies_item_id = ''
            _replies_owner_id = ''
            print('Error: item_id not found ! ') 

        tTextTag = iPostTag.findAll('', nTextTag)
        if len(tPostTag) > 0:
            for iTextTag in tTextTag:
                print(iTextTag.text)
                print('\n____________________________________________________________\n')

        else:
            print(iPostTag.text)
            print('\n_x___________________________________________________________\n')

        _replies_offset = 0
        _replies_top_replies = ''
        _replies_prev_id = ''

        tRepliesTag = iPostTag.find('', nRepliesTag )
        if tRepliesTag != None:
            _replies_top_replies = tRepliesTag.attrs['data-top-ids']

            tRepliesContTag = tRepliesTag.findAll('', nRepliesContTag )
            if len(tRepliesContTag) > 0:
                for iRepliesContTag in tRepliesContTag:
                    
                    match = re.match( r'^post(-\d*)_(\d*)', iRepliesContTag.attrs['id'] ) 

                    if match:
                        _replies_prev_id = match.group(2)
                    else:
                        print('Error: reply item_id not found ! ') 

                    _replies_offset += 1
                    tRepliesTextTag = iRepliesContTag.find('', nRepliesTextTag )
                    print(tRepliesTextTag.text)
                    print('\n       --------------------------------------------------------\n')

        #----begin------------ нажатие на кнопку "Показать ответы"
        tShowDeepRepliesTag = iPostTag.findAll('', nShowDeepRepliesTag )
        if False & len(tShowDeepRepliesTag) > 0:
            
            match = re.match( r'^replies_short_deep(-\d*)_(\d*)', tShowDeepRepliesTag[0].attrs['id'] ) 

            if match:
                _replies_owner_id = match.group(1)
                _replies_item_id = match.group(2)

            else:
                _replies_item_id = ''
                _replies_owner_id = ''
                print('Error: item_id not found (deep replies getting) ! ') 

            _replies_prev_id = ''
            _replies_offset = 0;

            _num_read_replies = 20

            _params_post = {'act': 'get_post_replies', 
                            'al': '1',
                            'item_id': _replies_item_id,
                            'offset': str(_replies_offset),
                            'order': 'smart',
                            'owner_id': _replies_owner_id,
                            }  

            if _replies_offset != 0:
                _params_post['count'] = str(_num_read_replies)
                _params_post['prev_id'] = str(_replies_prev_id)

            _replies_offset += _num_read_replies 

            _seek_next_replyes = True
            
            while _seek_next_replyes:
                data = session.post('https://vk.com/al_wall.php', headers = headers, data=_params_post)

                soup = BeautifulSoup(data.text.replace('\\', ''), "html.parser")
        
                tRepliesContTag = soup.findAll('', nRepliesContTag )
                if len(tRepliesContTag) > 0:
                    for iRepliesContTag in tRepliesContTag:
                        tRepliesTextTag = iRepliesContTag.find('', nRepliesTextTag )
                        if tRepliesTextTag != None:
                            print(tRepliesTextTag.text)
                            print('\n       --xxxx----------------------------------xxxxx-----------\n')
                        else:
                            pass #отработать другой формат сообщения 
                else:
                    _seek_next_replyes = False

                _params_post = {'act': 'get_post_replies', 
                                'al': '1',
                                'item_id': _replies_item_id,
                                'offset': str(_replies_offset),
                                'order': 'smart',
                                'owner_id': _replies_owner_id,
                                }  

                if _replies_offset != 0:
                    _params_post['count'] = str(_num_read_replies)
                    _params_post['prev_id'] = str(_replies_prev_id)

                _replies_offset += _num_read_replies 


                #tShowDeepRepliesTag = soup.findAll('', nShowNextRepliesTag )
        #----end------------ нажатие на кнопку "Показать ответы"

        #----begin------------ нажатие на кнопку "Показать следующие комментарии"
        tShowNextRepliesTag = iPostTag.find('', nShowNextRepliesTag )

        if False & (tShowNextRepliesTag != None):
            _num_read_replies = 20

            _params_post = {'act': 'get_post_replies', 
                          'al': '1',
                          'count': str(_num_read_replies),
                          'item_id': _replies_item_id,
                          'offset': str(_replies_offset),
                          'order': 'smart',
                          'owner_id': _replies_owner_id,
                          'prev_id': str(_replies_prev_id),
                          'top_replies': _replies_top_replies
                         }  

            _replies_offset += _num_read_replies 
            data = session.post('https://vk.com/al_wall.php', headers = headers, data=_params_post)
        
            soup = BeautifulSoup(data.text.replace('\\', ''), "html.parser")
        
            tRepliesTag = soup.find('', nRepliesTag )
            if tRepliesTag != None:

                tRepliesContTag = soup.findAll('', nRepliesContTag )
                if len(tRepliesContTag) > 0:
                    for iRepliesContTag in tRepliesContTag:
                        tRepliesTextTag = iRepliesContTag.find('', nRepliesTextTag )
                        if tRepliesTextTag != None:
                            print(tRepliesTextTag.text)
                            print('\n       --.....----------------------------------.....-----------\n')
                        else:
                            pass #отработать другой формат сообщения 
        #----end------------ нажатие на кнопку "Показать следующие комментарии"



       # _params_post['offset']      = str(_search_group_offset)
       # _params_post['real_offset'] = str(_search_group_offset)

       
        
        #tRepliesTag = iPostTag.find('', { 'class' : nRepliesTag })
        #if len(tRepliesTag) > 0:
        #    tRepliesContTag = tRepliesTag.find('', { 'class' : nRepliesContTag })
        #    if len(tRepliesContTag) > 0:
        #        tRepliesTextTag = tRepliesContTag.find('', { 'class' : nRepliesTextTag })
        #        if len(tRepliesTextTag) > 0:
        #            print(tRepliesTextTag.text)
        #            print('\n       --------------------------------------------------------\n')




            #tagBlocks = tagBody.findAll(re.compile('^img'), { 'class' : re.compile('search_item_img') })
            #for tagBlock in tagBlocks:
            #    print(str(self.count) + ' group id: '+tagBody.attrs['data-id'] + '     group name: '+tagBlock.attrs['alt'])
            #    groups_list.append({ 'id'   : tagBody.attrs['data-id'], 
            #                         'name' : tagBlock.attrs['alt'],
            #                         'screen_name' : '',
            #                         'is_closed' : 0
            #                       })
   #match = re.search(r'"query_id":"\d+"', data.text) 

    #if match:
    #    qid = data.text[match.regs[0][0]+12:match.regs[0][1]-1]

    #data.html.render()

    #time.sleep(1)

    #param_dict = {'act': 'show_more', 
    #              'al': '1',
    #              'al_ad': '0',
    #              'c[q]': 'пенз',
    #              'c[section]': 'communities',
    #              'offset': '40',
    #              'query_id': qid,
    #              'real_offset': '40'
    #             }  

    #data = session.post(url, headers=headers, data=param_dict)
    ##data = session.get(url, headers=headers, data=param_dict)     data.html.find('labeled title', first=True)  data._html.lxml
    
    #txt = data.text.replace('\\', '')
    
    #search_record_tag = 'groups_row search_row clear_fix'
    #first_search_tag = '<div class="' + search_record_tag

    #match = re.search(r'<!--.+?'+first_search_tag, txt) 

    #if match:
    #    txt = txt[match.regs[0][1]-len(first_search_tag):]

    f=1

#def crawl_pypeeter(login, password):

#    launch()
#    page = browser.newPage()
#    page.goto('http://example.com')
#    browser.close()

#    #async def localfun():
#    #    browser = await launch()
#    #    page = await browser.newPage()
#    #    await page.goto('http://example.com')
#    #    #await page.screenshot({'path': 'example.png'})
#    #    await browser.close()

#    asyncio.get_event_loop().run_until_complete(localfun())

if __name__ == "__main__":

    #s = 'post-16758516_113608'

    #s = re.sub('^post-', '', s)
    #s = re.sub('_\d*', '', s)

    #match = re.search('^post-\d*_', s ) 

    ##s[match.regs[0][1]:]

    #if match:
    #    f=1

    crawl_endless_scroll_wall()
    sys.exit(0)



    #all_combinations = combinations(('we','type','sort'), 2)
    ##print(all_combinations)
    #for comb in all_combinations:
    #    print(comb)
    #sys.exit(0)

   #for comb in all_combinations:
   #     #yield ' '.join(comb)
   #     yield ''.join(comb)


    #Crawler = crawler_vk(login = '89273824101', password = get_psw_mtyurin(), base_search_words = ['пенз'], msg_func = print)
    #groups_search_default(Crawler.api, 'пенз ио')
    #groups_search(Crawler.api, 'пенз ио', 'group', 0)
    #groups_search(Crawler.api, 'пенз ио', 'page' , 0)
    #groups_search(Crawler.api, 'пенз ио', 'event', 0)
    #groups_search(Crawler.api, 'пенз ио', 'group', 1)
    #groups_search(Crawler.api, 'пенз ио', 'group', 2)

    #ScrapGroups()  groups_search(api, pq, ptype, psort)

    #crawl_endless_scroll('89273824101', get_psw_mtyurin())

    #sys.exit(0)

    #crawl_pypeeter('89273824101', get_psw_mtyurin())


    #raise SystemExit(1)

    #s = 'qwe'
    #print(''.join((' '+i) for i in s ))
    #print(' '.join(i for i in s))

    #a = ord('а')
    #print(''.join([chr(i) for i in range(a,a+32)]))

    # ''.join([chr(i) for i in range(a,a+6)] + [chr(a+33)] + [chr(i) for i in range(a+6,a+32)])
  
    CassDB = pgree.CassandraDB(password=pgree.get_psw_mtyurin())
    CassDB.Connect()

    Crawler = crawler_vk(login = '89273824101', 
                         password = get_psw_mtyurin(), 
                         base_search_words = ['пенз'], 
                         msg_func = print, 
                         add_db_func = CassDB.AddToBD_SocialNet
                         )
    Crawler.id_cash = CassDB.SelectGroupsID()

    #Crawler._crawl_groups_browse('пенз')

    Crawler.crawl_groups() #by API

    CassDB.CloseConnection()


    #https://vk.com/search?c%5Bsection%5D=communities


    #for base_word in Crawler.gen_base_search_words():
    #    for w in Crawler.gen_search_words(base_word):
    #        print(w)
    #        if w == 'пенза п':
    #            for w2 in Crawler.gen_search_words(w):
    #                print(w2)

    #Crawler.GetVkToken('89273824101', get_psw_mtyurin())




    ##str_list = Crawler.gen_rus_alph_enum(1)

    ##print(str_list)
    
    #с = 0

    #for w in Crawler.gen_rus_alph_enum(2):
    #    с += 1
    #    print(w)

    #print(с)

    ##for w in Crawler.get_base_search_words():
    ##    print(w)
    
    #api = get_vkapi()

    ##read_wall(api, 421797609)

    ##user_search(api)

    #database_getRegions(api)

    #groups_search(api)



