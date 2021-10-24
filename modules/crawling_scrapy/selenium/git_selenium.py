from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

import time
import os

PATH_TO_CROMEDRIVER = os.path.dirname(os.path.abspath(__file__))+'\\chromedriver'

class EmptyWebElement:
    text = ''

class SelenDriver:
    def __init__(self, *args, **kwargs):
        options = Options()
        options.headless = True
        #options.add_argument("--window-size=1920,1200")
        self.driver = webdriver.Chrome(options=options, executable_path=PATH_TO_CROMEDRIVER)
        self.logger_err = None
        
    def get(self, url, show_more_func = None):
        time_sleep = 4
        tries = 3
        while tries>0:
            try:
                self.driver.get(url)
                if not show_more_func is None:
                    show_more_func()
                element = WebDriverWait(self.driver, 10).until(ec.presence_of_element_located((By.TAG_NAME, "html")))
                time.sleep(time_sleep)  #TODO smart waiter
                break
            except:
                tries -= 1
                time_sleep += 4
                if tries<=0:
                    if not self.logger_err is None:
                        logger_err.critical(f"Selenium can't load url: {url}")
                    raise
 
    def xpath_first_elem(self, xpath_request, selenium_webelem = None):
        try:
            if selenium_webelem is None:
                elem = self.driver.find_element_by_xpath(xpath_request)
            else:
                elem = selenium_webelem.find_element_by_xpath(xpath_request)
            return elem
        except:
            return EmptyWebElement()
        
    def xpath_first_text(self, xpath_request, selenium_webelem = None):
        try:
            if selenium_webelem is None:
                elem = self.driver.find_element_by_xpath(xpath_request)
            else:
                elem = selenium_webelem.find_element_by_xpath(xpath_request)
            return elem.text
        except:
            return ''
        
        
