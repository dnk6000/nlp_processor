# -*- coding: utf-8 -*-

# filename: crawler.py


import urllib  
from html.parser import HTMLParser  
from urllib.parse import urlparse
import urllib.request

import unicodedata

import requests

import re

import time
import datetime

import CrawlModulesPG.const as const
import CrawlModulesPG.date as date


class HREFParser(HTMLParser):  

    """

    Parser that extracts hrefs

    """

    hrefs = set()

    def handle_starttag(self, tag, attrs):

        if tag == 'a':

            dict_attrs = dict(attrs)

            if dict_attrs.get('href'):

                self.hrefs.add(dict_attrs['href'])
        pass


def get_local_links(html, domain):  

    """

    Read through HTML content and returns a tuple of links

    internal to the given domain

    """

    hrefs = set()

    parser = HREFParser()

    try: # иначе выскакивает ошибка на страницах-картинках в модуле _markupbase.py
        parser.feed(html)
    except:
        return hrefs

    for href in parser.hrefs:

        u_parse = urlparse(href)

        if href.startswith('/'):

            # purposefully using path, no query, no hash
            hrefs.add(u_parse.path)

        else:

          # only keep the local urls

          if (u_parse.netloc == domain) | (domain in u_parse.netloc):

            hrefs.add(u_parse.path)

    return hrefs


class Crawler(object):  

    def __init__(self, url, depth=2, pause_sec=1, ProcessedPages = []):

        """

        depth: how many time it will bounce from page one (optional)

        url: where we start crawling, should be a complete URL like

        'http://www.intel.com/news/'

        no_cache: function returning True if the url should be refreshed

        """ 

        self.depth = depth

        self.content = {}

        self.pause_sec = pause_sec  #tm
        
        self.u_parse = urlparse(url)

        self.domain = self.u_parse.netloc

        self.content[self.domain] = {}

        self.scheme = self.u_parse.scheme

        for url in ProcessedPages:
            parseurl = urlparse(url)
            if parseurl.path != '':
                self.set(parseurl.path, '')

    def crawl(self, urls=None, max_depth=None):
        
        #first call initialization
        if max_depth==None:
            max_depth=self.depth
        if urls==None:
            urls = [self.u_parse.path]
        
        n_urls = set()
        if max_depth:
            for url in urls:
                # do not crawl twice the same page
                if url not in self.content[self.domain]:
                    time.sleep(self.pause_sec)
                    html = self.get(url)
                    self.set(url, html)
                    n_urls = n_urls.union(get_local_links(html, self.domain))
                    yield (self.domain, url, html)
                else:
                    #print('!!! Найдена скрауленная страница: {}'.format(url))
                    pass

            for url_html in self.crawl(n_urls, max_depth-1):
               yield url_html
        pass



    def set(self, url, html):

        self.content[self.domain][url] = '' # html


    def get(self, url):

        page = self.curl(url)

        return page


    def curl(self, url):

        """

        return content at url.

        return empty string if response raise an HTTPError (not found, 500...)

        """

        try:

            print ("retrieving url... [{}] {}".format(self.domain, url))

            #req = urllib.request.Request('{}://{}{}'.format(self.scheme, self.domain, url))

            #response = urllib.request.urlopen(req)

            #resp_read = response.read()

            #return resp_read.decode('utf-8', 'ignore')

            req = requests.get('{}://{}{}'.format(self.scheme, self.domain, url))
            
            return req.content.decode('utf-8', 'ignore')

        #except urllib.request.HTTPError as e:
            #print("error {} {}: {}".format(self.domain, url, e))
        except Exception as e:

            print("{} error {} {}: {}".format(type(e),self.domain, url, e))

            return '';
        pass


class SnRecrawlerCheker:
    def __init__(self, cass_db = None, 
                       id_www_sources = None, 
                       id_project = None, 
                       sn_id = None, 
                       recrawl_days_post = None, 
                       recrawl_days_reply = None, 
                       plpy = None):

        self.post_reply_dates = dict()
        self.group_upd_date = const.EMPTY_DATE
        self.group_last_date = const.EMPTY_DATE  #last activity date
        self.str_to_date = date.StrToDate('%Y-%m-%d %H:%M:%S+.*')

        if cass_db is not None:
            res = cass_db.get_sn_activity(id_www_sources, id_project, sn_id, recrawl_days_post)

            _td = datetime.timedelta(days=recrawl_days_reply)

            for i in res:
                _post_id = i['sn_post_id']
                _upd_date = self._get_date(i['upd_date'])
                if _post_id == '':
                    self.group_upd_date = _upd_date
                    self.group_last_date = self._get_date(i['last_date'])
                else:
                    #if _upd_date is None or _upd_date == const.EMPTY_DATE:
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
        '''== reply already crawled ? '''
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


def RemoveEmojiSymbols(text):

   emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U0001F910-\U0001F978"  # smiles and other
                           "]+", flags=re.UNICODE)
    
   return emoji_pattern.sub(r'', text)


def remove_empty_symbols(text):
    
    txt = unicodedata.normalize("NFKD",text)   #solves the problem with simbol \xa0
    txt = re.sub(chr(10), ' ', txt)
    txt = re.sub('\x00', ' ', txt)
    txt = re.sub('\n+', ' ', txt)
    txt = re.sub(' +', ' ', txt)
    
    return txt

def Demo1():

    """
    демо вызова страничек пошагово - для каждого сайта можно делать свой вызов

    """

    #crawler = Crawler('http://techcrunch.com/', depth = 2, pause_sec = 5)
    #crawler = Crawler('http://toscrape.com/', depth = 2, pause_sec = 5)
    #crawler = Crawler('http://ria.ru/', depth = 2, pause_sec = 2)
    #crawler = Crawler('http://rbc.ru/', depth = 2, pause_sec = 2)
    #crawler = Crawler('https://rbc.ru/ekb/interview/06/04/2020/5e8b4ca29a79470833f82c76', depth = 2, pause_sec = 2)
    crawler = Crawler('https://vk.com/arendapnz2018', depth = 2, pause_sec = 2)
   
    crawling = crawler.crawl()

    res = next(crawling)
    domain = res[0]
    url    = res[1]
    html   = res[2]
    print(domain, url)
    print(html)
    print('______________________________________________________________________')
    #with open('hhhh.html', 'w', encoding='UTF-8') as f:
    #    f.write(html)

    #res = next(crawling)
    #domain = res[0]
    #url    = res[1]
    #html   = res[2]
    #print(domain, url)
    #print(html)
    #print('______________________________________________________________________')

    #res = next(crawling)
    #domain = res[0]
    #url    = res[1]
    #html   = res[2]
    #print(domain, url)
    #print(html)
    #print('______________________________________________________________________')

    pass

def Demo2():

    """
    демо вызова страничек циклом - так менее удобно, т.к. труднее запараллелить обход нескольких сайтов
    
    """

    #crawler = Crawler('http://techcrunch.com/', depth = 2, pause_sec = 5)
    #crawler = Crawler('http://toscrape.com/', depth = 2, pause_sec = 5)
    #crawler = Crawler('http://ria.ru/', depth = 2, pause_sec = 5)
    #crawler = Crawler('http://rbc.ru/', depth = 2, pause_sec = 2)
    crawler = Crawler('https://rbc.ru/ekb/interview/06/04/2020/5e8b4ca29a79470833f82c76', depth = 2, pause_sec = 2)
    
    crawling = crawler.crawl()

    for res in crawling:
        domain = res[0]
        url    = res[1]
        html   = res[2]
        print('______________________________________________________________________')
        print(domain, url)
        print('______________________________________________________________________')
        print(html)



if __name__ == "__main__":
    
    #    чтобы не подключаться к Postgree из Python есть идея возвращать за каждое обращение к функции - одну страничку
    #    Python запоминает свое состояние после каждого вызова и при следующем продолжает обход
    #    важно понять - сможем ли мы так вызывать из postgree - как сделано в Demo1()  
    #    т.е. будет ли запоминаться состояние выполнения при вызове из Postgree
    #    //пауза в данном случае внутри Python будет не нужна - ее нужно реализовать в Postgree

    Demo1()
    #Demo2()




