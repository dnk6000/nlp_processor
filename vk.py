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

import sys

from html.parser import HTMLParser  

from bs4 import BeautifulSoup

import time

#import asyncio
#from pyppeteer import launch


class social_net_crawler():

    def __init__(self, base_search_words = None, msg_func = None, add_db_func = None):

        if base_search_words == None:
            self.base_search_words = ['пенза', 'penza', 'pnz']
        else:
            self.base_search_words = base_search_words
        
        self.local_service_folder = 'C:\\Temp\\'

        #russian
        ia = ord('а')
        ii = ord('й')
        iz = ord('я')
        self.alph_str = ''.join([chr(i) for i in range(ia,ii)]+[chr(i) for i in range(ii+1,iz+1)])  #без букв й , ё
        #alph_str = ''.join([chr(i) for i in range(a,a+6)] + [chr(a+33)] + [chr(i) for i in range(a+6,a+32)]) # с буквами й , ё
        self.alph_str[0] += '\\'

        #english
        a = ord('a')
        self.alph_str.append(''.join([chr(i) for i in range(a,a+26)]))
        self.alph_str[1] += '\\'

        self.msg_func = msg_func        # функция для сообщений
        self.add_db_func = add_db_func  # функция для добавления в БД

        self.api_request_pause_sec = 1  # пауза между запросами к API
        self.api_limit_res = 1000       # максимум записей возвращаемых API


    def gen_base_search_words(self):
        
        for base_word in self.base_search_words:
            yield base_word


    def gen_rus_alph_enum(self, level = 1):
        
        #all_combinations = combinations_with_replacement(alph_str, level) #с повторением
        for _alph_str in self.alph_str:
            all_combinations = combinations(_alph_str, level)

            for comb in all_combinations:
                yield ' '.join(comb)

    def gen_search_words(self, word):
        
        for alph in self.gen_rus_alph_enum(1):
            if not ' '+alph in word:
                yield word+' '+alph
        
    def msg(self, message):

        if not self.msg_func == None:
            try:
                self.msg_func(message)
            except:
                pass

    def crawl_groups(self):

        for base_word in self.gen_base_search_words():
            self._crawl_groups(base_word)

    def _crawl_groups(self, start_word):
        
        for search_word in Crawler.gen_search_words(start_word):
            self.msg('Поиск групп по строке поиска: '+search_word)
            res = self._crawl_groups_api(search_word)
            if res['count'] >= self.api_limit_res: #это максимум что выдает
                self.msg('Количество результатов: '+str(res['count'])+' превышает максимум: '+str(self.api_limit_res))
                self._crawl_groups(search_word)
            else:
                self._add_groups_to_db(res['groups_list'])

    def _crawl_groups_api(self, search_word):

        self.msg('Search groups by API: '+search_word)
        return { 'count': 0, 'groups_list': list() }

    def _add_groups_to_db(self, groups_list):

        if self.add_db_func == None:
            self.msg('Add groups to BD')
            return 

        for i in groups_list:
            self.add_db_func(i)


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

    def _crawl_groups_api(self, search_word):

        sleep(self.api_request_pause_sec)
        groups = self.api.groups.search(q = search_word, count = self.api_limit_res)
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
        
        for i in range(300):

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
            #self.msg('Add groups to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(Gr['id']) + ' ' + Gr['name'])
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



def get_vkapi():
    
    #api = vk_requests.create_api(app_id=123, login='User', password='Password')

    #user token
    api = vk_requests.create_api(service_token="9aa969beea9a93677185fb52f3025bcdfa06210fc001a07b33ef777293e548d872c21f20483e0c980e1a0")

    with open('C:\\Temp\\vktoken.txt', 'r') as f:
        lservice_token = f.read()
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

def groups_search(api):

    groups = api.groups.search(q = 'пенза')

    pass
 

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

    #ScrapGroups()

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

    Crawler = crawler_vk(login = '89273824101', password = get_psw_mtyurin(), base_search_words = ['пенз'], msg_func = print, add_db_func = CassDB.AddToBD_SocialNet)

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



