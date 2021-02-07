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

class CrawlerCommon(common.CommonFunc):
	def __init__(self, 
			     need_stop_cheker = None, 
				 requests_delay_sec = 2,
				 request_error_pauser = None, 
				 **kwargs):
		
		super().__init__(**kwargs)

		self.need_stop_checker = need_stop_cheker
		self.requests_pauser = pauser.IntervalPauser(delay_seconds = requests_delay_sec)
		self.request_error_pauser = request_error_pauser

class Telegram(CrawlerCommon):
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
		
		self.request_counter = 1 #DEBUG
		while True:
			self.debug_msg('################ REQUEST MSG - '+str(self.request_counter))
			self.debug_msg(str(req_message_params))
			self.debug_msg('################')
			if self.debug_mode and self.request_counter > 20: #DEBUG
				return
			request_counter += 1 #DEBUG

			self.requests_pauser.smart_sleep()
			history = self.client(GetHistoryRequest(**req_message_params))

			if not history.messages:
				return

			req_message_params['offset_id'] = history.messages[len(history.messages) - 1].id #for next request
			for message in history.messages:
				if message.message == None:
					continue

				self.debug_msg('______________MESSAGE MESSAGE_____________________________')
				self.debug_msg('ID = '+str(message.id)+'	'+crawler.RemoveEmojiSymbols(message.message))
				self.debug_msg(str(message.date))
				self.debug_msg(str(message.replies))

				self.scrape_result.add_message(message, self.get_message_url(message.id, req_message_params['peer']),  id_group)
				#_message.sender_id
				#_message.views
				
				#Replyes
				if message.replies != None and message.replies.replies > 0:
					self._crawling_replies(req_reply_params, id_group, message.id)
		
			yield self.scrape_result.to_json() #return result per one post-request
		return

	def _crawling_replies(self, req_reply_params, id_group, id_message):
		req_reply_params['msg_id'] = id_message
		offset_reply = 0
		while True:
			self.requests_pauser.smart_sleep()
			self.debug_msg('################ REQUEST REPLY - '+str(self.request_counter))
			self.debug_msg(str(req_reply_params))
			self.debug_msg('################')
			if self.debug_mode and request_counter > 20: #DEBUG
				return
			self.request_counter += 1  #DEBUG

			history_repl = self.client(GetRepliesRequest(**req_reply_params))
			if not history_repl.messages:
				break
			for reply in history_repl.messages:
				self.debug_msg('________________REPLY REPLY___________________________')
				self.debug_msg('ID = '+str(reply.id)+'	Reply to ID = ' + str(reply.reply_to_msg_id) + '	' + crawler.RemoveEmojiSymbols(reply.message))
				self.debug_msg(str(reply.date))
				self.debug_msg(str(reply.replies))
						
				self.scrape_result.add_reply(reply, self.get_message_url(message.id, req_message_params['peer']),  id_group, id_message)

			req_reply_params['offset_id'] = history_repl.messages[len(history_repl.messages) - 1].id

	def get_message_url(self, id_message, url_group):
		return url_group+'/'+str(id_message)


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
	
	def add_reply(self, reply, url, group_id, message_id):

		super().add_result_type_REPLY(
			url = url,
			sn_id = group_id,
			sn_post_id = str(reply.id),
			sn_parent_id = str(message_id),
			author = str(reply.sender_id),
			content_date = date.date_to_str(reply.date),
			content = reply.message
			)		
