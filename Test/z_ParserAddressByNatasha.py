# -*- coding: utf-8 -*-

import crawler as CasCrawl  #for Demo

import re
from html.parser import HTMLParser  

from bs4 import BeautifulSoup

from natasha import AddressExtractor
from natasha import LocationExtractor
from natasha.markup import show_markup, show_json

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
            
            return _ResText


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
            
            return _ResText


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

def Demo1():

    """
    демо скрапинга 1 страницы

    """

    #crawler = CasCrawl.Crawler('https://ria.ru/20200226/1565161566.html', depth = 2, pause_sec = 1)
    crawler = CasCrawl.Crawler('https://www.rbc.ru/society/07/03/2020/5e63f6c99a7947d5acf23dac?from=from_main', depth = 2, pause_sec = 1)
 
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

            print('___________________________________________________________')
            extractor = AddressExtractor()

            matches = extractor(ScrapText)
            spans = [_.span for _ in matches]
            facts = [_.fact.as_json for _ in matches]
            show_markup(ScrapText, spans)
            show_json(facts)
    
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
    #crawler = CasCrawl.Crawler('http://ria.ru/', depth = 2, pause_sec = 2)
    #crawler = CasCrawl.Crawler('https://lenta.ru/', depth = 2, pause_sec = 1)
    crawler = CasCrawl.Crawler('http://rbc.ru/', depth = 2, pause_sec = 2)
   
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
 
def Demo3():

    #extractor = AddressExtractor()
    extractor = LocationExtractor()

    text = '''
        Трое человек, у которых обнаружили заражение коронавирусом в Липецкой области, прибыли из Италии в начале марта и добирались до области на личном автомобиле, 
        рассказали РБК в Управлении здравоохранения области.
        Заболевшие — женщина 1961 года рождения, а также супружеская пара — мужчина 1972 года рождения и женщина 1981 года рождения.
        Все они находились под наблюдением с момента возвращения в область и при первых симптомах были госпитализированы в инфекционный стационар, уточнили в Управлении.
        Кроме того, выявлено пять людей, которые контактировали с заболевшими. По данным областного Управления здравоохранения, сейчас они находятся в обсервационных отделениях.
        Ранее в субботу власти Липецкой области сообщали, что состояние больных «ближе к удовлетворительному». 
        Их госпитализировали в региональную клиническую инфекционную больницу и поместили в отдельные боксы.
        Одновременно с тремя жителями Липецкой области, врачи выявили коронавирус у жительницы Санкт-Петербурга. 
        Ее состояние удовлетворительное, болезнь протекает в легкой форме, передает ТАСС со ссылкой на Комитет по здравоохранению Санкт-Петербурга.        
        '''
    text = '''
        Предлагаю вернуть прежние границы природного парка №71 на Инженерной улице 2.

        По адресу Алтуфьевское шоссе д.51 (основной вид разрешенного использования: производственная деятельность, склады) размещен МПЗ. Жители требуют незамедлительной остановки МПЗ и его вывода из района

        Контакты О нас телефон 7 881 574-10-02 Адрес Республика Карелия,г.Петрозаводск,ул.Маршала Мерецкова, д.8 Б,офис 4
        '''
    matches = extractor(text)
    spans = [_.span for _ in matches]
    facts = [_.fact.as_json for _ in matches]
    show_markup(text, spans)
    show_json(facts)

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

    crawler = CasCrawl.Crawler(domain, depth, pause_sec)
   
    crawling = crawler.crawl()

    DetailedResult = True  
    #DetailedResult = False  

    #domain = None

    #clear log file
    flog = open(LogFileName, 'w', encoding='UTF-8')
    flog.close()

    Count = 1000

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
                #print(ScrapHeader)
                #print(ScrapText)
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

    Demo3()
    #Demo2()
    #CheckScraping('https://ria.ru', depth = 20, pause_sec = 2)
    #CheckScraping('https://rbc.ru', depth = 2, pause_sec = 2)
   
