# -*- coding: utf-8 -*-

import const
import crawler
import exceptions

import re
from html.parser import HTMLParser  

from bs4 import BeautifulSoup

from datetime import datetime

import json

class StrToDate:
    '''re_patterns - 're'-patterns str or list with  str:
        - 'dd mmm в hh:mm' ~ 20 ноя в 12:30
        - 'dd mmm yyyy' ~ 20 ноя 2019
        url, msg_func - for error exceptions
        str_date_format - format str from 'datetime' library
    '''
    MONTHSHORTSt = ['янв','фев','мар','апр','мая','июн','июл','авг','сен','окт','ноя','дек']
    MONTHSHORTSs = '|'.join(MONTHSHORTSt)
    REPATTERNS = {
        'dd mmm в hh:mm' : '(?P<day>\d\d?) (?P<monthshort>'+MONTHSHORTSs+') в (?P<hour>\d\d?):(?P<minute>\d\d)', #15 янв в 10:40
        'dd mmm yyyy'    : '(?P<day>\d\d?) (?P<monthshort>'+MONTHSHORTSs+') (?P<year>\d\d\d\d)',                 #15 янв 2019
        'сегодня в hh:mm': '(?P<day>сегодня) в (?P<hour>\d\d?):(?P<minute>\d\d)'                                 #сегодня в 10:40
        }

    def __init__(self, re_patterns = '', url = '', msg_func = None, str_date_format = ''):
        if str_date_format =='':
            self.str_date_format = "%d.%m.%Y %H:%M:00"
        else:
            self.str_date_format = str_date_format
        self.allowed_re_patterns = {}
        self.url = url 
        self.msg_func = msg_func 

        if isinstance(re_patterns, str):
            if re_patterns == '':
                for i in self.REPATTERNS:
                    self.allowed_re_patterns[i] = re.compile(self.REPATTERNS[i])
            else:
                self.allowed_re_patterns[re_patterns] = re.compile(self.REPATTERNS[re_patterns])

        elif isinstance(re_patterns, list) or isinstance(re_patterns, tuple):
            for i in re_patterns:
                self.allowed_re_patterns[i] = re.compile(self.REPATTERNS[i])

    def get_date(self, date_in_str, type_res = 'S'):
        _res_date_in_datetime = const.EMPTY_DATE 

        for re_pattern in self.allowed_re_patterns:  
            match = self.allowed_re_patterns[re_pattern].match(date_in_str)
            if match:
                res = match.groupdict()
                
                if 'monthshort' in res:
                    month = int(self.MONTHSHORTSt.index(match.group('monthshort'))) + 1
                else:
                    month = 1

                if 'day' in res:
                    if match.group('day') == 'сегодня':
                        _dt = datetime.now()
                        day = _dt.day
                        month = _dt.month
                    else:
                        day = int(match.group('day'))
                else:
                    day = 1
                
                if 'year' in res:
                    year = int(match.group('year'))
                else:
                    year = datetime.now().year

                hour = 0 if not 'hour' in res else int(match.group('hour'))
                minute = 0 if not 'minute' in res else int(match.group('minute'))

                try:
                    _res_date_in_datetime = datetime(year, month, day, hour, minute)
                except:
                    raise exceptions.ScrapeDateError(self.url, 'Error by scraping date from str "'+date_in_str+'"', self.msg_func)

                break

        _res_date_in_str = self._get_formated_date(_res_date_in_datetime)

        if type_res == 'S,D':
            return _res_date_in_str, _res_date_in_datetime
        else:
            return _res_date_in_str

    def _get_formated_date(self, dt):
        if self.str_date_format == '':
            return dt
        else:
            return dt.strftime(self.str_date_format)  

def date_to_str(dt):
    return dt.strftime("%d.%m.%Y %H:%M:%S")

def date_now_str():
    date_to_str(datetime.now())

class Scraper():

    def __init__(self, DetailedResult = False):
        
        self._DetailedResult = DetailedResult
        
    def ScrapingUniversal(self, html, 
                 articleHeaderTag,
                 articleBodyTag,
                 articleTextTag,
                 dateTag):
    
        ResList = list()

        return ResList


    def ScrapingRIA(self, html):

        articleHeaderTag   = '^article__title|^b-longread__widget-text m-input_title_article'
        articleAnnounceTag = '^article__announce-text'
        articleBodyTag     = '^article__body|^b-longread$'
        articleTextTag     = '^article__text|^b-longread__row'
        articleDateTag     = '^article__info-date'

        soup = BeautifulSoup(html, "html.parser")
    
        #tagsArticleBody = soup.findAll(re.compile('^article__body'))
        #tagsArticleBody = soup.findAll(re.compile('^div'), { 'class' : re.compile('^'+articleBodyTag) })
        #tagsArticleHeader = soup.findAll(re.compile('^h1'), { 'class' : re.compile('^'+articleHeaderTag) })
        #soup.findAll('', { 'class' : re.compile('^article__body|^b-longread\b') })
        
        tagsArticleBody     = soup.findAll('', { 'class' : re.compile(articleBodyTag) })
        tagsArticleAnnounce = soup.findAll('', { 'class' : re.compile(articleAnnounceTag) })
        tagsArticleHeader   = soup.findAll('', { 'class' : re.compile(articleHeaderTag) })
        tagsArticleDate     = soup.findAll('', { 'class' : re.compile(articleDateTag) })

        #__________________________
        def _ScrapingRIA_getDate():

            _ResDate = None

            if len(tagsArticleDate) > 0:
                strDate = tagsArticleDate[0].get_text()
                datePattern = r'\d\d\.\d\d\.\d\d\d\d'
                matchRes = re.search(datePattern, strDate)
                if matchRes != None: 
                    _ResDate = matchRes[0]

            return _ResDate

        #__________________________
        def _ScrapingRIA_getHeader():

            if len(tagsArticleHeader) > 0:
                _ResHeader = tagsArticleHeader.pop(0).get_text()
            else:
                _ResHeader = ''

            return _ResHeader

        #__________________________
        def _ScrapingRIA_getMainText(tagBody):
            _ResText = ''
            
            tagBlocks = tagBody.findAll(re.compile('^div'), { 'class' : re.compile('^'+articleTextTag) })
            if len(tagBlocks) != 0:
                for tagBlock in tagBlocks:
                    _ResText += tagBlock.get_text() + '\n'
            
            #если текст не найден - возьмем из анонса к видео ролику
            if (_ResText == '') & (len(tagsArticleAnnounce) > 0):
                _ResText = tagsArticleAnnounce[0].get_text()
            
            return crawler.remove_empty_symbols(_ResText)


        ResList = list()

        ResDate = _ScrapingRIA_getDate()

        for tagBody in tagsArticleBody:
            #tagBlocks = tagBody.findAll(re.compile('^div'), { 'class' : re.compile('^article__block'), 'data-type' : re.compile('^text') })

            ResHeader = _ScrapingRIA_getHeader()

            ResText = _ScrapingRIA_getMainText(tagBody)

            if self._DetailedResult: 
                ResList.append((ResText,ResHeader,ResDate))
            else:
                ResList.append(ResText)

        return ResList
    
    def ScrapingRBC(self, html):

        articleHeaderTag   = '^article__header__title'
        #articleBodyTag     = '^article__text |^article__text$'
        articleBodyTag     = '^article '
        #articleAnnounceTag = articleBodyTag
        #articleTextTag1     = 'article__content'
        articleTextTag1     = '^article__text |^article__text$'
        articleTextTag2     = '^ul$|^p$'
        articleDateTag     = '^article__header__date'

        soup = BeautifulSoup(html, "html.parser")
    
        #tagsArticleBody = soup.findAll(re.compile('^article__body'))
        #tagsArticleBody = soup.findAll(re.compile('^div'), { 'class' : re.compile('^'+articleBodyTag) })
        #tagsArticleHeader = soup.findAll(re.compile('^h1'), { 'class' : re.compile('^'+articleHeaderTag) })
        #soup.findAll('', { 'class' : re.compile('^article__header__date') })
        
        tagsArticleBody     = soup.findAll('', { 'class' : re.compile(articleBodyTag) })
        #tagsArticleAnnounce = soup.findAll('', { 'class' : re.compile(articleAnnounceTag) })
        tagsArticleHeader   = soup.findAll('', { 'class' : re.compile(articleHeaderTag) })
        tagsArticleDate     = soup.findAll('', { 'class' : re.compile(articleDateTag) })

        #__________________________
        def _ScrapingRBC_getDate():

            _ResDate = None

            if len(tagsArticleDate) > 0:
                if 'content' in tagsArticleDate[0].attrs:
                    strDate = tagsArticleDate[0].attrs['content']
                    datePattern = r'\d\d\d\d-\d\d-\d\d'
                    matchRes = re.search(datePattern, strDate)
                    if matchRes != None: 
                        _ResDate = matchRes[0]

            return _ResDate

        #__________________________
        def _ScrapingRBC_getHeader():

            if len(tagsArticleHeader) > 0:
                _ResHeader = tagsArticleHeader.pop(0).get_text()
            else:
                _ResHeader = ''

            return _ResHeader

        #__________________________
        def _ScrapingRBC_getMainText(tagBody):
            _ResText = ''
            tagBlocks1 = tagBody.findAll('', re.compile('l-col-center-590 article__content'))
            
            tagBlocks1 = tagBody.findAll('', re.compile(articleTextTag1))
            if len(tagBlocks1) != 0:
                for tagBlock1 in tagBlocks1:
                    tagBlocks = tagBlock1.findAll(re.compile(articleTextTag2))
                    if len(tagBlocks) != 0:
                        for tagBlock in tagBlocks:
                            _ResText += tagBlock.get_text() + '\n'
            
            #если текст не найден - возьмем из анонса к видео ролику
            #if (_ResText == '') & (len(tagsArticleAnnounce) > 0):
            #    _ResText = tagsArticleAnnounce[0].get_text()
            
            return crawler.remove_empty_symbols(_ResText)


        ResList = list()

        ResDate = _ScrapingRBC_getDate()

        for tagBody in tagsArticleBody:
            #tagBlocks = tagBody.findAll(re.compile('^div'), { 'class' : re.compile('^article__block'), 'data-type' : re.compile('^text') })

            ResHeader = _ScrapingRBC_getHeader()

            ResText = _ScrapingRBC_getMainText(tagBody)

            if self._DetailedResult: 
                ResList.append((ResText,ResHeader,ResDate))
            else:
                ResList.append(ResText)

        return ResList

    def ScrapingUniversal(self, html):
        
        pars = MyHTMLParser(True,self._DetailedResult)
        pars.feed(html)
        
        return pars.TextList

    def Scraping(self, html, domain = None ):
        
        if domain == None:
            listArticles = self.ScrapingUniversal(html)
            return listArticles

        elif 'ria.ru' in domain:
            listArticles = self.ScrapingRIA(html)
            return listArticles

        elif 'rbc.ru' in domain:
            listArticles = self.ScrapingRBC(html)
            return listArticles

        else:
            listArticles = self.ScrapingUniversal(html)
            return listArticles
            #return ('Err: Unknown domain for scraping','','')

class MyHTMLParser(HTMLParser):

    def __init__(self, convert_charrefs = True, DetailedResult = False):

        self.TextList = list()
        self._DetailedResult = DetailedResult

        super().__init__()

    def handle_data(self, data):
        if isRusProposal(data):
            if self._DetailedResult:
                self.TextList.append((data, '', ''))
            else:
                self.TextList.append(data)
            #print("Data     :", data)

    #def handle_starttag(self, tag, attrs):
    #    print("Start tag:", tag)
    #    for attr in attrs:
    #        print("     attr:", attr)

    #def handle_endtag(self, tag):
    #    print("End tag  :", tag)


    #def handle_comment(self, data):
    #    print("Comment  :", data)

    #def handle_entityref(self, name):
    #    c = chr(name2codepoint[name])
    #    print("Named ent:", c)

    #def handle_charref(self, name):
    #    if name.startswith('x'):
    #        c = chr(int(name[1:], 16))
    #    else:
    #        c = chr(int(name))
    #    print("Num ent  :", c)

    def handle_decl(self, data):
        #print("Decl     :", data)
        pass

def isRusProposal(Proposal):
    #pattern = r"а[а-яА-Я]в"
    #pattern = r"а[А-Ё]в"
    #pattern = r"[а-яА-Я]{5,30}\s*[а-яА-Я]{5,30}\s*[а-яА-Я]{5,30}"
    #s = re.sub('[Ёё]', 'е', s)
    #match = re.match(pattern, s)
    #print(match)

    CountEngWords = 50

    s = re.sub('[Ёё]', 'е', Proposal)
    
    patternEng = r"[a-zA-Z]{5,30}\s*"  #шаблон + цикл ниже = CountEngWords eng слов не менее чем из 5 букв - означают что это НЕ русское предложение

    count = 0
    for i in re.finditer(patternEng, s):
        count += 1
        #print(i)
        if count == CountEngWords: return False
        

    patternRus = r"[а-яА-Я]{5,30}\s*"  #шаблон + цикл ниже = три русских слова не менее чем из 5 букв - означают что это русское предложение

    count = 0
    for i in re.finditer(patternRus, s):
        count += 1
        #print(i)
        if count == 3: break
    
    return count >= 3


class TagNode:
    def __init__(self, 
                 process_func = None, 
                 result_func = None, 
                 func_par = None):
        """ process_func - function to process 'incoming parameters' in class function Scan
            result_func  - function to process result of process_func
            func_par - parameters for result_func
               required parameters:
                    tag_key -  regular expression with tag name for search in html
                    multi   -  True - find tag multi times, False - once
        """
        self.process_func = process_func
        self.result_func = result_func
        self.func_par = func_par

    def __repr__(self):
        return str(self.__dict__)

class TagTree():
    def __init__(self, nodes = None):
        """the type of node type must be 'list', 'TagNode' or None (default)
        """
        
        if type(nodes) == list:
            self.nodes = nodes
        elif type(nodes) == TagNode:
            self.nodes = [nodes]
        else:
            self.nodes = list()
        
        self.childs = list()

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

    def scan(self, par1, par2 = {}):
        
        #result_of_process_func = None 

        for node in self.nodes:
            if node.process_func != None:
                proc_par1, proc_par2 = node.process_func(par1, par2, **node.func_par)
            
            if node.result_func != None:
                res_par1, res_par2 = node.result_func(proc_par1, proc_par2, **node.func_par)
            else:
                res_par1, res_par2 = proc_par1, proc_par2

        #if hasattr(result_of_result_func, '__iter__'):
        if issubclass(type(res_par1), list):
            for i_res_par1 in res_par1:
                for child in self.childs:
                    child.scan(i_res_par1, res_par2)
        else:
            for child in self.childs:
                child.scan(res_par1, res_par2)
            
    
    def set_par(self, par_id, par_value):
        '''set parameter value in dict 'func_par' for all tree nodes
        '''
        
        for node in self.nodes:
            node.func_par[par_id] = par_value

        for child in self.childs:
            child.set_par(par_id, par_value)


class ScrapeResult:
    def __init__(self, clear_result = True):
        self.clear_result = clear_result

    def get_json_result(self, result):
        json_result = json.dumps(result)
        if self.clear_result:
            result.clear()
        return json_result

def Demo_getProcessedPages():
    listProcessedPages = []

    listProcessedPages.append('https://ria.ru/specialprojects/')
    listProcessedPages.append('https://ria.ru/incidents/')
    listProcessedPages.append('https://ria.ru/lenta/')
    listProcessedPages.append('https://ria.ru/author_danilov/')
    listProcessedPages.append('https://ria.ru/science/')
    listProcessedPages.append('https://ria.ru/economy/')
    listProcessedPages.append('https://ria.ru/video/')
    listProcessedPages.append('https://ria.ru/infografika/')
    listProcessedPages.append('https://ria.ru/longread/')
    listProcessedPages.append('https://ria.ru/authors/')
    listProcessedPages.append('https://ria.ru/author_krasheninnikova/')
    listProcessedPages.append('https://ria.ru/specialprojects/')
    listProcessedPages.append('https://ria.ru/tags/')
    listProcessedPages.append('https://ria.ru/religion/')
    listProcessedPages.append('https://ria.ru/caricature/')
    listProcessedPages.append('https://ria.ru/society/')
    listProcessedPages.append('https://ria.ru/author_alksnis/')

    return listProcessedPages


def Demo1():

    """
    демо скрапинга 1 страницы

    """

    #crawler = Crawler('http://techcrunch.com/', depth = 2, pause_sec = 5)
    #crawler = Crawler('http://toscrape.com/', depth = 2, pause_sec = 5)
    #crawler = Crawler('http://ria.ru/', depth = 2, pause_sec = 1)
    #crawler = crawler.Crawler('https://ria.ru/20200220/1564972897.html', depth = 2, pause_sec = 1)
    #crawler = crawler.Crawler('https://ria.ru/20200226/1565250161.html', depth = 2, pause_sec = 1)
    #crawler = crawler.Crawler('https://lenta.ru/', depth = 2, pause_sec = 1)
    #crawler = crawler.Crawler('https://www.rbc.ru/finances/28/02/2020/5e58e3a89a7947320d17337a?from=from_main', depth = 2, pause_sec = 1)
    #crawler = crawler.Crawler('https://ria.ru/20200226/1565161566.html', depth = 2, pause_sec = 1)
    #crawler = crawler.Crawler('https://www.rbc.ru/society/03/03/2020/5e2fe9459a79479d102bada6?from=from_main', depth = 2, pause_sec = 1)
    #crawler = crawler.Crawler('https://www.rbc.ru/politics/04/03/2020/5e5e5d7e9a79472a06eb61ad', depth = 2, pause_sec = 1)
    #crawler = crawler.Crawler('https://www.ria.ru/20200323/1568890618.html', depth = 2, pause_sec = 1, FunIsPageProcessed = IsPageProcessedScrap)
    crawler = crawler.Crawler('https://www.rbc.ru/v10_rbcnews_static/rbcnews-10.2.30/images/rbc-logo.eps', depth = 2, pause_sec = 1)

    crawling = crawler.crawl()

    res = next(crawling)
    domain = res[0]
    url    = res[1]
    html   = res[2]
    print(domain, url)
    #print(html)
    #print('______________________________________________________________________')

    DetailedResult = True  
    #DetailedResult = False  

    #domain = None

    if DetailedResult: 
        #возвращать список статей, каждая запись списка = кортеж из трех значений - текст, заголовок, дата
        scraper = Scraper(True)
 
        ScrapGen = scraper.Scraping(html, domain)

        for ScrapRes in ScrapGen:
            ScrapText   = ScrapRes[0]
            ScrapHeader = ScrapRes[1]
            ScrapDate   = ScrapRes[2]
            print(ScrapHeader)
            print(ScrapDate)
            print(ScrapText)
     
    else:
        #возвращать список статей, каждая запись списка = текст
        scraper = Scraper()
 
        ScrapGen = scraper.Scraping(html, domain)

        for ScrapRes in ScrapGen:
            ScrapText = ScrapRes
            print(ScrapText)

def Demo2():

    """
    демо скрапинга всего сайта
    
    """

    #crawler = Crawler('http://techcrunch.com/', depth = 2, pause_sec = 5)
    #crawler = Crawler('http://toscrape.com/', depth = 2, pause_sec = 5)
    #crawler = crawler.Crawler('http://ria.ru/', depth = 2, pause_sec = 2)
    #crawler = crawler.Crawler('https://lenta.ru/', depth = 2, pause_sec = 1)
    crawler = crawler.Crawler('http://rbc.ru/', depth = 2, pause_sec = 2)
   
    crawling = crawler.crawl()

    DetailedResult = True  
    #DetailedResult = False  

    #domain = None

    Count = 20

    for res in crawling:
        domain = res[0]
        url    = res[1]
        html   = res[2]
        print('______________________________________________________________________')
        print(domain, url)
        #print('______________________________________________________________________')
        #print(html)

        if DetailedResult: 
            #возвращать список статей, каждая запись списка = кортеж из трех значений - текст, заголовок, дата
            scraper = Scraper(True)
 
            ScrapGen = scraper.Scraping(html, domain)

            if len(ScrapGen) == 0:
                print('Article not found: ', domain + url)

            for ScrapRes in ScrapGen:
                ScrapText   = ScrapRes[0]
                ScrapHeader = ScrapRes[1]
                ScrapDate   = ScrapRes[2]
                #print(ScrapHeader)
                #print(ScrapText)
                Err = (ScrapText == '') | (ScrapHeader == '') | (ScrapDate == '')
                if True | Err:
                    #print('Not all tags found: ',domain, url)
                    print('#Header: ', ScrapHeader)
                    print('#Date: ', ScrapDate)
                    print('#Text: ', ScrapText)
     
        else:
            #возвращать список статей, каждая запись списка = текст
            scraper = Scraper()
 
            ScrapGen = scraper.Scraping(html, domain)

            for ScrapRes in ScrapGen:
                ScrapText = ScrapRes
                print(ScrapText)



        Count -= 1
        if Count == 0: break
    pass
 
def CheckScraping(domain, depth, pause_sec):

    """
    проверка скрапинга всего сайта
    выдаются страницы в которых не найдены все искомые теги
    
    """

    LogFileName = 'CheckScraping_Log.txt'

    def AddToLogFile(MsgStr):
        flog = open(LogFileName, 'a', encoding='UTF-8')
        flog.write(msgstr)
        flog.close()

    _crawler = crawler.Crawler(domain, depth, pause_sec, ProcessedPages = Demo_getProcessedPages())
   
    crawling = _crawler.crawl()

    DetailedResult = True  
    #DetailedResult = False  

    #domain = None

    #clear log file
    flog = open(LogFileName, 'w', encoding='UTF-8')
    flog.close()

    Count = 200

    SuccessCount = 0

    for res in crawling:
        domain = res[0]
        url    = res[1]
        html   = res[2]
        #print('______________________________________________________________________')
        #print(domain, url)
        #print('______________________________________________________________________')
        #print(html)

        if DetailedResult: 
            #возвращать список статей, каждая запись списка = кортеж из трех значений - текст, заголовок, дата
            scraper = Scraper(True)
 
            ScrapGen = scraper.Scraping(html, domain)

            SuccessCount += 1

            if len(ScrapGen) == 0:
                msgstr = '### Article not found: %s%s \n' % (domain, url)
                
                print(msgstr)
                
                AddToLogFile(msgstr)
                
                SuccessCount -= 1


            for ScrapRes in ScrapGen:
                ScrapText   = ScrapRes[0]
                ScrapHeader = ScrapRes[1]
                ScrapDate   = ScrapRes[2]
                print(ScrapHeader)
                print(ScrapText)
                Err = (ScrapText == '') | (ScrapHeader == '') | (ScrapDate == '')
                if Err:
                    msgstr = '### Not all tags found: %s%s \n' % (domain, url)
                    msgstr += '     #Header: %s' % (ScrapHeader)
                    msgstr += '     #Date: %s' % (ScrapDate)
                    msgstr += '     #Text: %s' % (ScrapText)
                    print(msgstr)
                    AddToLogFile(msgstr)
                    SuccessCount -= 1
     
        print('Success counter: ',SuccessCount)
        Count -= 1
        if Count == 0: break
    
    AddToLogFile('Success counter: %s'.format(SuccessCount))



if __name__ == "__main__":

    #Demo1()
    #Demo2()
    #CheckScraping('https://ria.ru', depth = 20, pause_sec = 2)
    #CheckScraping('https://rbc.ru', depth = 5, pause_sec = 2)

    s = StrToDate()

    ss = s.get_date("31 июл в 21:47")
    ss = s.get_date("сегодня в 10:42")
    f=1
   
