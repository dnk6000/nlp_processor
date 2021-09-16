#import scrapy
#from scrapy.crawler import CrawlerProcess

##myself
#from git_scrapy.spiders.tripadv import TripAdvisorSpider

#process = CrawlerProcess({
#    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
#})

#process.crawl(TripAdvisorSpider)
#process.start() # the script will block here until the crawling is finished

import Modules.CrawlingScrapy.spiders
from Modules.CrawlingScrapy.spiders.tripadv import TripAdvisorSpider

scraper = git_scrapy.spiders.RunSpider(TripAdvisorSpider)
scraper.run_spiders()
