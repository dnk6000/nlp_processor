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

	def __init__(self, id_group, date_deep, **kwargs):
		super().__init__(**kwargs)
		self.id_group = id_group

		self.repeats = 20 #DEBUG

		self.date_deep = datetime.datetime(date_deep.year, date_deep.month, date_deep.day, date_deep.hour, date_deep.minute, date_deep.second, tzinfo = datetime.timezone.utc)

		self.scrape_result = ScrapeResultTG(**kwargs)

		self._url = r'https://t.me/'

		self.activity_registrator = ActivityRegistrator()


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
		def debug_msg_local_1(): #DEBUG
			if not self.debug_mode:
				return
			nonlocal req_message_params
			self.debug_msg('################ REQUEST MSG - '+str(self.request_counter))
			self.debug_msg(str(req_message_params))
			self.debug_msg('################')
			self.request_counter += 1						
		def debug_msg_local_2(): #DEBUG
			if not self.debug_mode:
				return
			nonlocal message
			self.debug_msg('______________MESSAGE MESSAGE_____________________________')
			self.debug_msg('ID = '+str(message.id)+'	'+crawler.RemoveEmojiSymbols(message.message))
			self.debug_msg(str(message.date))
			self.debug_msg(str(message.replies))

		self.activity_registrator.initialize(id_group)
		req_reply_params = req_message_params.copy()
		self.stop_by_date_deep = False

		self.request_counter = 1								#DEBUG
		while True:
			debug_msg_local_1()									#DEBUG

			self.requests_pauser.smart_sleep()
			history = self.client(GetHistoryRequest(**req_message_params))

			if not history.messages:
				break

			req_message_params['offset_id'] = history.messages[len(history.messages) - 1].id #for next request
			for message in history.messages:
				if message.message is None:
					continue
				self.check_date_deep(message.date)
				if self.stop_by_date_deep:
					break

				debug_msg_local_2()									#DEBUG

				self.scrape_result.add_message(message, self.get_message_url(message.id, req_message_params['peer']),  id_group)
				#_message.sender_id
				#_message.views
				
				self.activity_registrator.registrate(message.id, message.date)

				#Replyes
				if message.replies is not None and message.replies.replies > 0:
					self._crawling_replies(req_reply_params, id_group, message.id)

			self.activity_registrator.move_to_scrape_result(self.scrape_result)		
			yield self.scrape_result.to_json() #return result per one post-request
			if self.stop_by_date_deep:
				self.debug_msg('STOP BY DEEP DATE	STOP BY DEEP DATE	STOP BY DEEP DATE	STOP BY DEEP DATE')
				break
		return

	def _crawling_replies(self, req_reply_params, id_group, id_message):
		def debug_msg_local_1(): #DEBUG
			if not self.debug_mode:
				return
			nonlocal req_reply_params
			self.debug_msg('################ REQUEST REPLY - '+str(self.request_counter))
			self.debug_msg(str(req_reply_params))
			self.debug_msg('################')
			self.request_counter += 1							#DEBUG
		def debug_msg_local_2(): #DEBUG
			if not self.debug_mode:
				return
			nonlocal reply
			self.debug_msg('________________REPLY REPLY___________________________')
			self.debug_msg('ID = '+str(reply.id)+'	Reply to ID = ' + str(reply.reply_to_msg_id) + '	' + crawler.RemoveEmojiSymbols(reply.message))
			self.debug_msg(str(reply.date))
			self.debug_msg(str(reply.replies))

		
		first_request = True
		req_reply_params['msg_id'] = id_message
		offset_reply = 0

		while True:
			self.requests_pauser.smart_sleep()

			debug_msg_local_1()									#DEBUG

			history_repl = self.client(GetRepliesRequest(**req_reply_params))
			if not history_repl.messages:
				break

			for reply in history_repl.messages:
				debug_msg_local_2()								#DEBUG
				self.scrape_result.add_reply(reply, self.get_reply_url(reply.id, id_message, req_reply_params['peer']),  id_group, id_message)
				if first_request:
					self.activity_registrator.registrate(id_message, reply.date)
					first_request = False

			req_reply_params['offset_id'] = history_repl.messages[len(history_repl.messages) - 1].id

	def get_message_url(self, id_message, url_group):
		return url_group+'/'+str(id_message)

	def get_reply_url(self, id_reply, id_message, url_group):
		return self.get_message_url(id_message, url_group) + '?comment=' + str(id_reply)

	def check_date_deep(self, dt):
		if self.date_deep != const.EMPTY_DATE and dt <= self.date_deep:
			self.stop_by_date_deep = True


class ScrapeResultTG(scraper.ScrapeResult):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def add_message(self, message, url, group_id):

		super().add_result_type_POST(
			url = url,
			#sn_id = group_id,
			sn_id = group_id[0:12], #DEBUG  CROP ID !!!
			sn_post_id = str(message.id),
			author = str(message.sender_id),
			content_date = date.date_to_str(message.date),
			content = message.message
			)		
	
	def add_reply(self, reply, url, group_id, message_id):

		super().add_result_type_REPLY(
			url = url,
			#sn_id = group_id,
			sn_id = group_id[0:12], #DEBUG
			sn_post_id = str(reply.id),
			sn_post_parent_id = str(message_id),
			author = str(reply.sender_id),
			content_date = date.date_to_str(reply.date),
			content = reply.message
			)		

class ActivityRegistrator(dict, common.CommonFunc):
	def __init__(self, *args, sn_id = '', tz = datetime.timezone.utc, **kwargs):
		super().__init__(*args, **kwargs)
		self.EMPTY_DATE = const.EMPTY_DATE_UTC if tz == datetime.timezone.utc else const.EMPTY_DATE
		self.initialize(sn_id)
		
	def initialize(self, sn_id):
		self.common_last_date = self.EMPTY_DATE
		self.sn_id = sn_id
		self.clear()

	def registrate(self, sn_post_id, dt):
		if dt is None or dt == self.EMPTY_DATE: 
			return

		if not sn_post_id in self:	    #first
			self[sn_post_id] = dt
		elif self[sn_post_id] < dt:	#register the latest datetime
			self[sn_post_id] = dt

		if self.common_last_date < dt:
			self.common_last_date = dt

	def move_to_scrape_result(self, scrape_result):
		for sn_post_id in self:
			scrape_result.add_result_type_activity(self.sn_id[0:12], str(sn_post_id), self[sn_post_id])  #DEBUG  CROP ID !!!
		scrape_result.add_result_type_activity(self.sn_id[0:12], '', self.common_last_date)				#DEBUG  CROP ID !!!
		self.clear()