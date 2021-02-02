import CrawlModulesPG.telegram1 as tg
import CrawlModulesPyOnly.plpyemul as plpyemul

import datetime
import time

def save_db():

    print('saving to db '+str(datetime.datetime.now()))
    time.sleep(1)

if __name__ == "__main__":

    plpy = plpyemul.PlPy()

    tg1 = tg.TelegramCrawler(func_save_to_db = save_db, msg_func = plpy.notice)
    tg1.main()

    pass




