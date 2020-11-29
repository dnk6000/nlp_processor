# -*- coding: utf-8 -*-
import sys
import traceback

class CrawlerException(Exception):
    pass

class CrawlVkError(CrawlerException):
    pass


class CrawlVkByBrowserError(CrawlVkError):
    
    def __init__(self, url = '', error_description = '', msg_func = None):
        self.url = url
        self.error_description = error_description
        self.sys_info = sys.exc_info()
        
        if msg_func != None:
            msg_func(self.url)
            msg_func(self.error_description)
            msg_func(self.sys_info)

    def __repr__(self):
        return self.__dict__

class ScraperException(Exception):
    pass

class ScrapeDateError(ScraperException):
    def __init__(self, url = '', error_description = '', msg_func = None):
        self.url = url
        self.error_description = error_description
        self.sys_info = sys.exc_info()
        
        if msg_func != None:
            msg_func(self.url)
            msg_func(self.error_description)
            msg_func(self.sys_info)

    def __repr__(self):
        return self.__dict__

def get_err_description(_exeption, **kwargs):
    _descr = ''
    _descr += '\n'.join(map(str, sys.exc_info()))
    _descr += traceback.format_exc()
    _descr += '\n'+' Exception: '+str(_exeption)
    _descr += '\n'+' kwargs: '+str(kwargs)
    return _descr
