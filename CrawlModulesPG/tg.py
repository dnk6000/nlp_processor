import asyncio
import time
import datetime

from telethon.sync import TelegramClient
from telethon import connection

# классы для работы с каналами
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

# класс для работы с сообщениями
from telethon.tl.functions.messages import GetHistoryRequest, GetRepliesRequest

import CrawlModulesPG.accounts as accounts
import CrawlModulesPG.crawler as crawler
import CrawlModulesPG.exceptions as exceptions
import CrawlModulesPG.const as const
import CrawlModulesPG.date as date
import CrawlModulesPG.pauser as pauser


class AsyncCrawler:
    def __init__(self,  manager_func = None, 
                        debug_mode = True,
                        msg_func = None):
        self.manager_func = manager_func
        self.msg_func     = msg_func
        self.debug_mode   = debug_mode
        self.tasks = [ asyncio.Task(self._crawling_task()) ]

    async def _crawling_task(self):
        pass

    async def _manager(self):
        if self.manager_func != None:
            self.manager_func()
        pass

    def start(self):
        self.loop = asyncio.get_event_loop()
        
        for task in self.tasks:
            task.is_manager = False

        _task_manager = asyncio.Task(self._manager())
        _task_manager.is_manager = True
        self.tasks.append(_task_manager)

        try:
            self.loop.run_until_complete(asyncio.wait(self.tasks))
        except:
            self.action_after_loop_exception()
        finally:
            self.loop.close()
            self.action_after_loop_finished()
            

    def msg(self, message):
        if not self.msg_func == None:
            try:
                self.msg_func(str(message))
            except:
                pass

    def debug_msg(self, message):
        if not self.debug_mode:
            return
        self.msg(message)

    def action_after_loop_exception(self):
        pass 

    def action_after_loop_finished(self):
        pass 


class Telegram(AsyncCrawler):
    def __init__(self, username, api_id, api_hash, **kwargs):
        super().__init__(**kwargs)

        self.username = username
        self.api_id = api_id
        self.api_hash = api_hash


    def connect(self):
        self.client = TelegramClient(session = self.username, api_id = self.api_id, api_hash = self.api_hash)
        self.client.start()




class TelegramMessagesCrawler(Telegram):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.repeats = 20
        self.tasks = [ asyncio.Task(self._crawling_task(2)), asyncio.Task(self._crawling_task(3)) ]

    async def _crawling_task(self, sec = 1):
        #while self.repeats > 0:
        #    self.repeats -= 1
        #    self.msg('crawling = {}  repeats = {}   time = {}'.format(sec, self.repeats, str(datetime.datetime.now())))
        #    await asyncio.sleep(sec)
        #pass
        _channel = r'https://t.me/' + id_group
		_offset_msg = 0
		_limit_msg = 100
		_total_messages = 0
		_request_counter = 0

		pause_sec = 1

		while True:
			time.sleep(pause_sec)
			_req_param = { 'peer': _channel, 
					  'offset_id':_offset_msg,
					  'offset_date': None, 
					  'add_offset': 0,
					  'limit': _limit_msg, 
					  'max_id': 0, 
					  'min_id': 0,
					  'hash': 0 }
			self._debug_msg('################ REQUEST MSG - '+str(_request_counter))
			self._debug_msg(str(_req_param))
			self._debug_msg('################')
			_request_counter += 1


			_history = await self.client(GetHistoryRequest(**_req_param))
			if not _history.messages:
				break
			for _message in _history.messages:
				self.messages.append(_message.to_dict())
				_total_messages += 1

				txt = crawler.RemoveEmojiSymbols(_message.message)

				self._debug_msg('______________MESSAGE MESSAGE_____________________________')
				self._debug_msg('ID = '+str(_message.id)+'	'+txt)
				self._debug_msg(str(_message.date))
				self._debug_msg(str(_message.replies))
				#_message.sender_id
				#_message.views

				if _message.replies != None and _message.replies.replies > 0:
					_offset_reply = 0
					while True:
						time.sleep(pause_sec)
						_req_param = { 'peer': _channel, 
								  'msg_id':_message.id,
								  'offset_id':_offset_reply,
								  'offset_date': None, 
								  'add_offset': 0,
								  'limit': _limit_msg, 
								  'max_id': 0, 
								  'min_id': 0,
								  'hash': 0 }
						self._debug_msg('################ REQUEST REPLY - '+str(_request_counter))
						self._debug_msg(str(_req_param))
						self._debug_msg('################')
						_request_counter += 1

						_history_repl = await self.client(GetRepliesRequest(**_req_param))
						if not _history_repl.messages:
							break
						for _reply in _history_repl.messages:
							self.messages.append(_reply.to_dict())
							_total_messages += 1
							txt = crawler.RemoveEmojiSymbols(_reply.message)

							self._debug_msg('________________REPLY REPLY___________________________')
							self._debug_msg('ID = '+str(_reply.id)+'	Reply to ID = ' + str(_reply.reply_to_msg_id) + '	' + txt)
							self._debug_msg(str(_reply.date))
							self._debug_msg(str(_reply.replies))
						_offset_reply = _history_repl.messages[len(_history_repl.messages) - 1].id

			_offset_msg = _history.messages[len(_history.messages) - 1].id
			
			#DEBUG
			if _total_messages >= 1000:
				break
		
		self.msg('Всего сообщений: '+ str(_total_messages))
		pass

    async def _manager(self):
        while self.repeats > 0:
            self.msg('managing.   repeats = {}   time = {}'.format(self.repeats, str(datetime.datetime.now())))
            if self.manager_func != None:
                self.manager_func()
            await asyncio.sleep(0.1)

    def action_after_loop_exception(self):
        pass #TODO finally save to DB

    def action_after_loop_finished(self):
        pass #TODO append to result critical error


