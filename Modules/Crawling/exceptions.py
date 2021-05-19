# -*- coding: utf-8 -*-
import sys
import traceback

class CrawlerException(Exception):
    pass

class CrawlVkError(CrawlerException):
    pass

class CrawlVkGetTokenError(CrawlVkError):
    pass


class CrawlVkByBrowserError(CrawlVkError):
    
    def __init__(self, url = '', error_description = '', msg_func = None):
        self.url = url
        self.error_description = error_description
        self.sys_info = sys.exc_info()
        
        if msg_func is not None:
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
        
        if msg_func is not None:
            msg_func(self.url)
            msg_func(self.error_description)
            msg_func(self.sys_info)

    def __repr__(self):
        return self.__dict__

class UserInterruptByDB(Exception):
    def __str__(self):
        return 'UserInterruptByDB'

class StopProcess(Exception):
    def __str__(self):
        return 'StopProcess'

class CrawlCriticalErrorsLimit(Exception):
    def __init__(self, number_of_errors):
        self.number_of_errors = number_of_errors


def get_err_description(_exeption, **kwargs):
    _descr = ''
    _descr += '\n'.join(map(str, sys.exc_info()))
    _descr += traceback.format_exc()
    _descr += '\n'+' Exception: '+str(_exeption)
    _descr += '\n'+' kwargs: '+str(kwargs)
    return _descr
