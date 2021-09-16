from scrapy.spiders import Spider
from scrapy.linkextractors import LinkExtractor

import re
from datetime import datetime

from urllib.parse import urldefrag, parse_qsl
import logging

from Modules.CrawlingScrapy.items import ScrapyTripadvisorItem 
from Modules.CrawlingScrapy.selenium.git_selenium import SelenDriver, EmptyWebElement
import Modules.Common.const as const
import Modules.Common.common as common

__log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"

def get_logger_result(name):
    logger = logging.getLogger(name+'_res')

    fname = const.LOG_FOLDER+name+"_Result.log"
    common.clear_file(fname)

    file_handler = logging.FileHandler(fname)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(__log_format))

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(__log_format))

    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

def get_logger_errors(name):
    logger = logging.getLogger(name+'_err')

    fname = const.LOG_FOLDER+name+"_ERR.log"
    common.clear_file(fname)

    file_handler = logging.FileHandler(fname)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(__log_format))

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(logging.Formatter(__log_format))

    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

logger_res = get_logger_result('ta')
logger_err = get_logger_errors('ta')

class TripAdvSelenDriver(SelenDriver):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.re_geo = re.compile("\|((\d|\.)+),((\d|\.)+)$")
        self.logger_err = logger_err

    def get_restaurant_item(self, url):
        self.get(url)

        #NavNext, NavNext_txt = self.find_element_by_xpath(r"//a[@class='ui_button nav next primary ']")
        #self.driver.find_element_by_xpath(rf"//span[@data-test-target='staticMapSnapshot']/img")   self.driver.find_element_by_xpath(rf"//ancestor::div[text()='Поблизости']")

        item = ScrapyTripadvisorItem()
        name = self.xpath_first_text(rf'//h1[@data-test-target="top-info-header"]')
        if '\n' in name:
            _ = name.split('\n')
            item['name']  = _[0]
            item['name2'] = _[1]
        else:
            item['name'] = name
            item['name2'] = ''


        position_web_elem = self.xpath_first_elem(rf"//span[@data-test-target='staticMapSnapshot']/img")

        if not isinstance(position_web_elem, EmptyWebElement):
            src = position_web_elem.get_attribute('src')
            geo = self.get_geo_coordinates(src)
            item['geo_latitude'] = geo[0]
            item['geo_longitude'] = geo[1]
            if geo[0] is None:
                logger_err.error(f'Geocoding failed: \n url: {url} \n src: {src}')
        else:
            logger_err.error(f'Geocoding tag not found: \n url: {url}')

        item['addres']  = self.xpath_first_text(r'//div[@id="atf_header_wrap"]//a[@href="#MAPVIEW"]')
        if item['addres'] == '':
            logger_err.error(f'Address not found: \n url: {url}')

        item['category']  = self.get_category_by_url(url)
        item['url']     = url
        
        return item

    def get_hotel_item(self, url):
        self.get(url)

        #NavNext, NavNext_txt = self.find_element_by_xpath(r"//a[@class='ui_button nav next primary ']")
        #self.driver.find_element_by_xpath(rf"//span[@data-test-target='staticMapSnapshot']/img")   self.driver.find_element_by_xpath(rf"//ancestor::div[text()='Поблизости']")

        item = ScrapyTripadvisorItem()
        name = self.xpath_first_text(rf"//h1[@id='HEADING']")
        if '\n' in name:
            _ = name.split('\n')
            item['name']  = _[0]
            item['name2'] = _[1]
        else:
            item['name'] = name
            item['name2'] = ''


        position_web_elem = self.xpath_first_elem(rf"//span[@data-test-target='staticMapSnapshot']/img")

        if not isinstance(position_web_elem, EmptyWebElement):
            src = position_web_elem.get_attribute('src')
            geo = self.get_geo_coordinates(src)
            item['geo_latitude'] = geo[0]
            item['geo_longitude'] = geo[1]
            if geo[0] is None:
                logger_err.error(f'Geocoding failed: \n url: {url} \n src: {src}')
        else:
            logger_err.error(f'Geocoding tag not found: \n url: {url}')

        item['addres']  = self.xpath_first_text(r'//div[@id="LOCATION"]//div[text()="Связаться"]/ancestor::*[1]//span[text() != ""]')
        item['category']  = self.get_category_by_url(url)
        item['url']     = url
        
        return item

    def get_attraction_item(self, url):
        self.get(url)

        #NavNext, NavNext_txt = self.find_element_by_xpath(r"//a[@class='ui_button nav next primary ']")
        #self.driver.find_element_by_xpath(rf"//span[@data-test-target='staticMapSnapshot']/img")   self.driver.find_element_by_xpath(rf"//ancestor::div[text()='Поблизости']")

        item = ScrapyTripadvisorItem()
        item['name'] = self.xpath_first_text(rf"//h1[@data-automation='mainH1']")

        position_web_elem = self.xpath_first_elem(rf"//span[@data-test-target='staticMapSnapshot']/img")

        if not isinstance(position_web_elem, EmptyWebElement):
            src = position_web_elem.get_attribute('src')
            geo = self.get_geo_coordinates(src)
            item['geo_latitude'] = geo[0]
            item['geo_longitude'] = geo[1]
            if geo[0] is None:
                logger_err.error(f'Geocoding failed: \n url: {url} \n src: {src}')
        else:
            logger_err.error(f'Geocoding tag not found: \n url: {url}')

        item['addres']  = self.xpath_first_text(r'//div[@data-automation="AppPresentation_PoiLocationSectionGroup"]//div[text()="Поблизости"]/ancestor::*[1]//button[@type="button"]/descendant::*')
        item['category']  = self.get_category_by_url(url)
        item['url']     = url
        
        return item

    def get_geo_coordinates(self, url):

        params = url.split('?')[1].split('&')

        geo_str = ''

        for i in params:
            x = i.split('=')
            if x[0] == 'markers':
                if len(x[1]) > len(geo_str):
                    geo_str = x[1]

        if geo_str != '':
            match = self.re_geo.search(geo_str)
            if match:
                return (match.groups()[0], match.groups()[2])

        return (None, None)

    def get_category_by_url(self, url):
        if 'Attraction' in url:
            return 'Attraction'
        elif 'Restaurant' in url:
            return 'Restaurant'
        elif 'Hotel' in url:
            return 'Hotel'
        else:
            return 'Unknown'


class TripAdvisorStartUrl():
    domain = 'www.tripadvisor.ru'
    
    region_tripadv_id = '298539'
    region_addon_url = 'Chelyabinsk_Chelyabinsk_Oblast_Urals_District'

    hotels_url      = rf'https://{domain}/Hotels-g{region_tripadv_id}-{region_addon_url}-Hotels.html'
    attractions_url = rf'https://{domain}/Attractions-g{region_tripadv_id}-Activities-{region_addon_url}.html'
    restaurants_url = rf'https://{domain}/Restaurants-g{region_tripadv_id}-{region_addon_url}.html'

    start_urls = []
    #start_urls.append(attractions_url)
    #start_urls.append(hotels_url)
    start_urls.append(restaurants_url)

    attractions_headers = [
        'Природа и парки',
        'Музеи',
        'Архитектурные достопримечательности',
        'Исторические достопримечательности',
        'Покупки',
        'Театры',
        'Исторические места для',
        'Достопримечательности и культурные объекты',
        'Специализированные музеи',
        'Концерты и представления'
        ]

    def get_absolute_url(self, relative_url):
        return r'https://' + self.domain + relative_url


class TripAdvisorSpider(Spider):
    name = "tripadvisor"
    allowed_domains = ["www.tripadvisor.ru"]

    source_cfg = TripAdvisorStartUrl()

    start_urls = source_cfg.start_urls.copy()
    
    debug_counter = 0

    selen_driver = TripAdvSelenDriver()

    logger_res.info('Start')

    def parse(self, response):
        self.debug_counter += 1
        #if self.debug_counter > 3:
        #    return

        if 'Attractions' in response.request.url:
            attractions_category_hrefs = self.scrap_attraction_start_page(response)
            for href in attractions_category_hrefs:
                logger_err.debug(f'go to: {href} ')
                next_page_url = self.source_cfg.get_absolute_url(href)
                yield response.follow(next_page_url, callback=self.parse_attraction_category)
        elif 'Hotels' in response.request.url:
            hotels_hrefs = self.scrap_hotels_list_page(response)
            for href in hotels_hrefs:
                #logger_res.info(f'{self.source_cfg.get_absolute_url(href)}')
                #pass
                hotel_url = self.source_cfg.get_absolute_url(href)
                item = self.selen_driver.get_hotel_item(hotel_url) #selenium
                yield item
        
            next_page_url = self.scrap_hotels_next_page_button(response)
            if not next_page_url is None:
                yield response.follow(next_page_url, callback=self.parse)
        elif 'Restaurant' in response.request.url:
            restaurants_hrefs = self.scrap_restaurants_list_page(response)
            for href in restaurants_hrefs:
                #logger_res.info(f'{self.source_cfg.get_absolute_url(href)}')
                #pass
                restaurant_url = self.source_cfg.get_absolute_url(href)
                item = self.selen_driver.get_restaurant_item(restaurant_url) #selenium
                yield item
        
            next_page_url = self.scrap_restaurants_next_page_button(response)
            if not next_page_url is None:
                yield response.follow(next_page_url, callback=self.parse)

        return

    def scrap_attraction_start_page(self, response):
        #parse attraction category
        attractions_category_hrefs = []
        for header in self.source_cfg.attractions_headers:
            href = response.xpath(rf"//a[@href and starts-with(text(),'{header}')]/@href").extract_first()
            if href is None:
                logger_err.error(f'href is None for "{header}"')
            else:
                attractions_category_hrefs.append(href)
        return attractions_category_hrefs

    def parse_attraction_category(self, response):
        logger_err.debug(f'PARSE_ATTRACTION_CATEGORY: {datetime.now()}')
        
        #parse attraction-cards href's
        hrefs = set(response.xpath(rf"//section[@data-automation='AppPresentation_SingleFlexCardSection']//a[@href][1]/@href").extract())
        if len(hrefs) == 0:
            logger_err.error(f'Attraction hrefs not found in list page: {response.url}')

        for href in hrefs:
            #print(f'Request: {datetime.now()}')
            next_page_url = self.source_cfg.get_absolute_url(href)
            item = self.selen_driver.get_attraction_item(next_page_url) #selenium
            yield item

        #parse attraction next page button
        href = response.xpath(rf"//section[@data-automation='AppPresentation_PaginationLinksList']//a[@href and @aria-label='Next page']/@href").extract_first()
        if not href is None:
            logger_err.debug(f'go to next page: {href}')
            next_page_url = self.source_cfg.get_absolute_url(href)
            yield response.follow(next_page_url, callback=self.parse_attraction_category)
        
        return


    def scrap_hotels_list_page(self, response):
        logger_err.debug(f'PARSE_HOTELS_LIST: {datetime.now()}')
        
        #parse hotels-cards href's
        hrefs = set(response.xpath(rf'//div[@class="relWrap"]//a[@data-clicksource="HotelName"]/@href').extract())
        if len(hrefs) == 0:
            logger_err.error(f'Hotels hrefs not found in list page: {response.url}')
        return hrefs

    def scrap_hotels_next_page_button(self, response):
        href = response.xpath(rf'//div[@class="unified ui_pagination standard_pagination ui_section listFooter"]//a[@href and text()="Далее"]/@href').extract_first()
        if not href is None:
            logger_err.debug(f'go to next page: {href}')
            next_page_url = self.source_cfg.get_absolute_url(href)
            return next_page_url
        return None


    def scrap_restaurants_list_page(self, response):
        logger_err.debug(f'PARSE_RESTAURANTS_LIST: {datetime.now()}')
        
        #parse hotels-cards href's
        hrefs = set(response.xpath(rf'//div[@id="EATERY_LIST_CONTENTS"]//div[contains(@data-test,"_list_item")]//a[@href and not(contains(@href,"#")) and not(contains(@href,"ShowUserReviews")) and not(contains(@href,"UserReviewEdit"))]/@href').extract())
        if len(hrefs) == 0:
            logger_err.error(f'Restaurants hrefs not found in list page: {response.url}')
        return hrefs

    def scrap_restaurants_next_page_button(self, response):
        href = response.xpath(rf'//div[@class="unified pagination js_pageLinks"]//a[@href and contains(text(),"Далее")]/@href').extract_first()
        if not href is None:
            logger_err.debug(f'go to next page: {href}')
            next_page_url = self.source_cfg.get_absolute_url(href)
            return next_page_url
        return None


