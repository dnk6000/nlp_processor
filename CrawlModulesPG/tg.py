import time
import datetime
import json

from telethon.sync import TelegramClient
from telethon import connection

# классы для работы с каналами
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

# класс для работы с сообщениями
from telethon.tl.functions.messages import GetHistoryRequest, GetRepliesRequest

import CrawlModulesPG.crawler as crawler
import CrawlModulesPG.exceptions as exceptions
import CrawlModulesPG.const as const
import CrawlModulesPG.date as date
import CrawlModulesPG.pauser as pauser
import CrawlModulesPG.scraper as scraper
import CrawlModulesPG.common as common

class Telegram(common.CommonFunc):
	def __init__(self, username, api_id, api_hash, **kwargs):
		super().__init__(**kwargs)

		self.username = username
		self.api_id = api_id
		self.api_hash = api_hash


	def connect(self):
		self.client = TelegramClient(session = self.username, api_id = self.api_id, api_hash = self.api_hash)
		self.client.start()

class TelegramMessagesCrawler(Telegram):

	def __init__(self, id_group, **kwargs):
		super().__init__(**kwargs)
		self.id_group = id_group

		self.repeats = 20 #DEBUG

		self.scrape_result = ScrapeResultTG(**kwargs)

		self._url = r'https://t.me/'

		pass

	def crawling(self, id_group):
		req_group_params = { 
			'peer': self._url + id_group, 
			'offset_id': 0,
			'offset_date': None, 
			'add_offset': 0,
			'limit': 0, 
			'max_id': 0, 
			'min_id': 0,
			'hash': 0
			}

		for step_result in self._crawling(req_group_params, id_group):
			yield step_result
		return

		try:
			for step_result in self._crawling(req_group_params):
				yield step_result
		#except requests.exceptions.RequestException as e:
		#	#TODO
		#	_descr = exceptions.get_err_description(e, _cw_url = self._cw_url)
		#	self._cw_add_to_result_critical_error(str(e), _descr)
		#	yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)  
		#	return
		except exceptions.UserInterruptByDB as e:
			#TODO
			self.debug_msg('UserInterruptByDB Exception')
			yield [{'result_type': 'UserInterruptByDB Exception'}]
			#_descr = exceptions.get_err_description(e, _cw_url = self._cw_url)
			#self._cw_add_to_result_critical_error(str(e), _descr, stop_process = True)
			#yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)  
			return
		except Exception as e:
			#TODO
			self.debug_msg('Unknown Exception')
			yield [{'result_type': 'Unknown Exception'}]
			#_descr = exceptions.get_err_description(e, _cw_url = self._cw_url)
			#self._cw_add_to_result_critical_error(str(e), _descr)
			#yield self._cw_res_for_pg.get_json_result(self._cw_scrape_result)  
			return

	def _crawling(self, req_message_params, id_group):
		req_reply_params = req_message_params.copy()
		total_messages = 0
		
		request_counter = 1 #DEBUG
		while True:
			self.debug_msg('################ REQUEST MSG - '+str(request_counter))
			self.debug_msg(str(req_message_params))
			self.debug_msg('################')
			if self.debug_mode and request_counter > 20: #DEBUG
				return
			request_counter += 1 #DEBUG

			history = self.client(GetHistoryRequest(**req_message_params))

			if not history.messages:
				return

			req_message_params['offset_id'] = history.messages[len(history.messages) - 1].id #for next request

			for message in history.messages:
				if message.message == None:
					continue

				#self.messages.append(message.to_dict())
				total_messages += 1

				txt = crawler.RemoveEmojiSymbols(message.message)

				self.debug_msg('______________MESSAGE MESSAGE_____________________________')
				self.debug_msg('ID = '+str(message.id)+'	'+txt)
				self.debug_msg(str(message.date))
				self.debug_msg(str(message.replies))

				self.scrape_result.add_message(message, self.get_message_url(message.id, req_message_params['peer']),  id_group)
				#_message.sender_id
				#_message.views
				

				#if message.replies != None and message.replies.replies > 0:
				#	req_reply_params['msg_id'] = message.id

				#	offset_reply = 0
				#	while True:
				#		time.sleep(1)
				#		self.debug_msg('################ REQUEST REPLY - '+str(request_counter))
				#		self.debug_msg(str(req_reply_params))
				#		self.debug_msg('################')
				#		if self.debug_mode and request_counter > 20: #DEBUG
				#			return
				#		request_counter += 1

				#		history_repl = self.client(GetRepliesRequest(**req_reply_params))
				#		if not history_repl.messages:
				#			break
				#		for reply in history_repl.messages:
				#			#self.messages.append(reply.to_dict())
				#			total_messages += 1
				#			txt = crawler.RemoveEmojiSymbols(_reply.message)

				#			self._debug_msg('________________REPLY REPLY___________________________')
				#			self._debug_msg('ID = '+str(_reply.id)+'	Reply to ID = ' + str(_reply.reply_to_msg_id) + '	' + txt)
				#			self._debug_msg(str(_reply.date))
				#			self._debug_msg(str(_reply.replies))
						
				#		req_reply_params['offset_id'] = history_repl.messages[len(history_repl.messages) - 1].id
		
			yield self.scrape_result.to_json()
		return

	def get_message_url(self, id_message, url_group):
		return url_group+'/'+str(id_message)



	def action_after_loop_exception(self):
		pass #TODO finally save to DB

	def action_after_loop_finished(self):
		pass #TODO append to result critical error

class ScrapeResultTG(scraper.ScrapeResult):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def add_message(self, message, url, group_id):

		super().add_result_type_POST(
			url = url,
			sn_id = group_id,
			sn_post_id = str(message.id),
			author = str(message.sender_id),
			content_date = date.date_to_str(message.date),
			content = message.message
			)		
