#from telethon.sync import TelegramClient

#client = TelegramClient(session = 'tg_api_key', api_id = '3756968', api_hash = '6f685fcaf0056c27f8b5d77ef1265f04')

#client.start()

import Modules.Crawling.tg as tg
#import Modules.Crawling.accounts as accounts

#try: 
#    msg_func = plpy.notice
#except:
#    msg_func = print

#tg_crawler = tg.Telegram(**accounts.TG_ACCOUNT[0], msg_func = msg_func, debug_mode = True)
#tg_crawler.connect()

tg.Telegram.key_generate_only()