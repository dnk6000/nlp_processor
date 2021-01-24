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




