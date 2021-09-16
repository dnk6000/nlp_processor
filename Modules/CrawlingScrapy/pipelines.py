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
        return item
