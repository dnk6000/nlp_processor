# -*- coding: utf-8 -*-
import sys

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
            msg_func(self.sys_info())

    def __repr__(self):
        return self.__dict__

