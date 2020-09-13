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

def crawl_endless_scroll_wall(): #!!!

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
    nFixedArea    = { 'class' : re.compile('wall_fixed') }
    
    nPostTag      = { 'class' : re.compile('^_post post.*') } 
    nTextTag      = { 'class' : re.compile('wall_text') } 

    nRepliesTag      = { 'class' : re.compile('^replies_list') }
    nRepliesContTag  = { 'class' : re.compile('^reply reply_dived') }
    nRepliesTextTag  = { 'class' : re.compile('^wall_reply_text') }

    #replies
    #replies_list
    #reply reply_dived
    #reply_text
    

    nShowNextRepliesTag  = { 'onclick' : re.compile('^return wall.showNextReplies') }

    nShowDeepRepliesTag  = { 'onclick' : re.compile('^return wall.openDeepShortReplies') }


    _post_counter = 0
    _post_fixed = ''

    tFixedArea     = soup.find('', nFixedArea )
    if tFixedArea != None:
        tPostTag     = tFixedArea.findAll('', nPostTag )
        if len(tPostTag) > 0:
            if len(tPostTag) > 1:
                print('Error: найдено несколько фикс. постов ')

            match = re.match( r'^post(-\d*)_(\d*)', tPostTag[0].attrs['id'] ) 

            if match:
                _post_fixed = match.group(2)
            else:
                print('Error: fixed item_id not found ! ') 
            
            _post_counter -= 1

    #tPostTag     = soup.findAll('', { 'class' : nPostTag })

    _scroll_enable =True

    while _scroll_enable:

        _scroll_post_count = 0

        tPostTag     = soup.findAll('', nPostTag )

        for iPostTag in tPostTag:
            _post_counter += 1
            _scroll_post_count += 1
            #if not 'Греф' in iPostTag.text:
            #if not 'К происходящему в Беларуси. Протесты в Беларуси. В чём там дело?' in iPostTag.text:
            #    continue

            match = re.match( r'^post(-\d*)_(\d*)', iPostTag.attrs['id'] ) 

            if match:
                _replies_owner_id = match.group(1)
                _replies_item_id = match.group(2)

            else:
                _replies_item_id = ''
                _replies_owner_id = ''
                print('Error: item_id not found ! ') 

            tTextTag = iPostTag.findAll('', nTextTag)
            if len(tTextTag) > 0:
                for iTextTag in tTextTag:
                    print(iTextTag.text)
                    print('\n____________________________________________________________\n')

            else:
                print(iPostTag.text)
                print('\n_x___________________________________________________________\n')


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


        #----begin------------ прокрутка страницы
        print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        print('  _post_counter == ' + str(_post_counter))

        time.sleep(2)

        if _scroll_post_count > 0 & (_scroll_post_count < 10):
            print('Warning: _scroll_post_count == ' + str(_scroll_post_count) + '  ' + url + '  _post_counter == ' + str(_post_counter))

        if _scroll_post_count > 0:
            _params_post = {'act': 'get_wall', 
                    'al': '1',
                    'fixed': _post_fixed,
                    'offset': str(_post_counter),
                    'onlyCache': 'false',
                    'owner_id': _replies_owner_id,
                    'type': 'own',
                    'wall_start_from': str(_post_counter + 1),
                    }  
            data = session.post('https://vk.com/al_wall.php', headers = headers, data=_params_post)
            
            txt = data.text.replace('\\', '')
            txt = txt.replace('<!--{"payload":[0,["', '')
            soup = BeautifulSoup(txt, "html.parser")
        else:
            _scroll_enable = False
        #----end------------ прокрутка страницы


       # _params_post['offset']      = str(_search_group_offset)
       #2 _params_post['real_offset'] = str(_search_group_offset)

       
        
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

def tst(a):
    print(a)
    print(id(a))


class CrawlerError(Exception):
    def __init__(self, text, url):
        self.error_txt = text
        self.url = url

    def __repr__(self):
        return self.__dict__

def e1():
    raise CrawlerError('Ошибка е1', 'www.vk.com')

def ttt():
    return 'ddddddd', 123

if __name__ == "__main__":

    #try:
    #    e1()
    #except Exception as e:
    #    print(e)

    a, b = ttt()

    print(a)
    print(b)

    sys.exit(0)

    r = { 'a' : 123 }
    print(r)
    print(id(r))
    print('---')


    tst(**r)
    sys.exit(0)

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


