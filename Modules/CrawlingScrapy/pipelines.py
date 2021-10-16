# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from Modules.CrawlingScrapy.spiders.tripadv import logger_res, logger_err


class GitScrapyPipeline:
    def process_item(self, item, spider):
        logger_res.info(f'\n № {item.get_instance_counter()} \n{str(item)}')

        longitude = item['geo_longitude'] if 'geo_longitude' in item else 0
        latitude = item['geo_latitude'] if 'geo_latitude' in item else 0
        name2 = item['name2'] if 'name2' in item else ''

        spider.db.upsert_trip_advisor(name = item['name'], 
                                        name_lemma = '', 
                                        name2 = name2, 
                                        address = item['address'], 
                                        category_str = item['category'], 
                                        longitude = longitude, 
                                        latitude = latitude, 
                                        url = item['url']
                                      )

        return item
