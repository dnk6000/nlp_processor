# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class ScrapyTripadvisorItem(scrapy.Item):
    instance_counter = 0

    name = scrapy.Field()
    name2 = scrapy.Field()

    geo_latitude = scrapy.Field()
    geo_longitude = scrapy.Field()

    addres = scrapy.Field()
    category = scrapy.Field()
    url  = scrapy.Field()

    def __init__(self, *args, **kwargs):
        self.__class__.instance_counter += 1
        return super().__init__(*args, **kwargs)

    @classmethod
    def get_instance_counter(cls):
        return cls.instance_counter