import CrawlModulesPG.tg as tg
import CrawlModulesPyOnly.plpyemul as plpyemul

import datetime
import time

def save_db():

    print('saving to db '+str(datetime.datetime.now()))
    time.sleep(1)

if __name__ == "__main__":

    plpy = plpyemul.PlPy()

    crawler = tg.TelegramMessagesCrawler(manager_func = save_db, msg_func = plpy.notice, id_groups = ['govoritfursov'], **accounts.TG_ACCOUNT[0])
    crawler.start()
    pass




