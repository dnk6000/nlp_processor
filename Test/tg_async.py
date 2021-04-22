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

import Crawling.crawler as crawler
import Crawling.exceptions as exceptions
import Common.const as const
import Crawling.date as date
import Crawling.pauser as pauser

class CommonFunc:
    def __init__(self, debug_mode = True, msg_func = None, **kwargs):
        self.msg_func     = msg_func
        self.debug_mode   = debug_mode

    def msg(self, message):
        if not self.msg_func is None:
            try:
                self.msg_func(str(message))
            except:
                pass

    def debug_msg(self, message):
        if not self.debug_mode:
            return
        self.msg(message)


class AsyncCrawler(CommonFunc):
    def __init__(self,  manager_func = None, tasks_fun = [], tasks_par = [], **kwargs):
        super().__init__(**kwargs)
        
        self.manager_func = manager_func
        self.tasks_fun = tasks_fun
        self.tasks_par = tasks_par


    async def loop(self):
        self.tasks = []
        for i in range(0, len(self.tasks_fun)):
            task = asyncio.create_task(self.tasks_fun[i](**self.tasks_par[i]))
            task.is_manager = False
            self.tasks.append(task)

        task_manager = asyncio.create_task(self.manager())
        task_manager.is_manager = True
        self.tasks.append(task_manager)

        while True:
            await task
            await task_manager

        #self.loop = asyncio.get_event_loop()
        
        #self.loop.run_until_complete(asyncio.wait(self.tasks))
        #try:
        #except:
        #    self.action_after_loop_exception()
            
    def start(self):
        #self.loop()
        asyncio.run(self.loop())
        pass
        #self.loop.close()
        #self.action_after_loop_finished()

    async def manager(self):
        #while self.repeats > 0:
        self.msg('managing.   time = {}'.format(str(datetime.datetime.now())))
        if self.manager_func is not None:
            self.manager_func()
        await asyncio.sleep(0.1)

    def action_after_loop_exception(self):
        if self.debug_mode:
            self.msg('EXCEPT EXCEPT')
        pass 

    def action_after_loop_finished(self):
        if self.debug_mode:
            self.msg('FINISH FINISH')
        pass 


class Telegram(CommonFunc):
    def __init__(self, username, api_id, api_hash, **kwargs):
        super().__init__(**kwargs)

        self.username = username
        self.api_id = api_id
        self.api_hash = api_hash


    def connect(self):
        self.client = TelegramClient(session = self.username, api_id = self.api_id, api_hash = self.api_hash)
        self.client.start()


class TelegramMessagesCrawler(Telegram):

	def __init__(self, id_groups = [], **kwargs):
		super().__init__(**kwargs)
		#self.id_group = id_group

		self.repeats = 20 #DEBUG

		self.tasks_fun = [ self.crawling for id_group in id_groups ]
		self.tasks_par = [ {'id_group': id_group} for id_group in id_groups ]
		pass

	async def crawling(self, id_group):
		req_group_params = { 
			'peer': r'https://t.me/' + id_group, 
			'offset_id': 0,
			'offset_date': None, 
			'add_offset': 0,
			'limit': 0, 
			'max_id': 0, 
			'min_id': 0,
			'hash': 0 }
		self.debug_msg('################')
		await asyncio.sleep(1)
		#while True:
		#	await asyncio.sleep(1)
			#await self._crawling(req_group_params)
		#try:
		#	await self._crawling(req_group_params)
		#except:
		#	pass
		#pass

	async def _crawling(self, req_message_params):
		req_reply_params = req_message_params.copy()
		total_messages = 0
		
		request_counter = 1
		while True:
			self.debug_msg('################ REQUEST MSG - '+str(request_counter))
			self.debug_msg(str(req_message_params))
			self.debug_msg('################')
			if self.debug_mode and request_counter > 20: #DEBUG
				return
			request_counter += 1

			history = await self.client(GetHistoryRequest(**req_message_params))
			if not _history.messages:
				break
			for message in history.messages:
				#self.messages.append(message.to_dict())
				total_messages += 1

				txt = crawler.RemoveEmojiSymbols(_message.message)

				self.debug_msg('______________MESSAGE MESSAGE_____________________________')
				self.debug_msg('ID = '+str(message.id)+'	'+txt)
				self.debug_msg(str(message.date))
				self.debug_msg(str(message.replies))
				#_message.sender_id
				#_message.views

				if message.replies is not None and message.replies.replies > 0:
					req_reply_params['msg_id'] = message.id

					offset_reply = 0
					while True:
						time.sleep(1)
						self.debug_msg('################ REQUEST REPLY - '+str(request_counter))
						self.debug_msg(str(req_reply_params))
						self.debug_msg('################')
						if self.debug_mode and request_counter > 20: #DEBUG
							return
						request_counter += 1

						history_repl = await self.client(GetRepliesRequest(**req_reply_params))
						if not history_repl.messages:
							break
						for reply in history_repl.messages:
							#self.messages.append(reply.to_dict())
							total_messages += 1
							txt = crawler.RemoveEmojiSymbols(_reply.message)

							self._debug_msg('________________REPLY REPLY___________________________')
							self._debug_msg('ID = '+str(_reply.id)+'	Reply to ID = ' + str(_reply.reply_to_msg_id) + '	' + txt)
							self._debug_msg(str(_reply.date))
							self._debug_msg(str(_reply.replies))
						
						req_reply_params['offset_id'] = history_repl.messages[len(history_repl.messages) - 1].id
		return





	def action_after_loop_exception(self):
		pass #TODO finally save to DB

	def action_after_loop_finished(self):
		pass #TODO append to result critical error


