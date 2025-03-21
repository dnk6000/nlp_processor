import time
import datetime
import json
import os

import telethon.version as telethon_version
from telethon.sync import TelegramClient
from telethon.sessions import StringSession #, SQLiteSession
from telethon.sessions.sqlite import EXTENSION as SQLiteSession_EXTENSION
#from telethon import connection
from telethon.errors.rpcerrorlist import MsgIdInvalidError, UsernameNotOccupiedError, ChannelPrivateError, UsernameInvalidError

from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import Channel, ChannelParticipantsSearch, InputPeerChat, InputPeerChannel
from telethon.tl.functions.contacts import SearchRequest

from telethon.tl.functions.messages import GetHistoryRequest, GetRepliesRequest

import modules.crawling.crawler as crawler
import modules.common_mod.exceptions as exceptions
import modules.common_mod.const as const
import modules.common_mod.date as date
import modules.common_mod.pauser as pauser
import modules.crawling.scraper as scraper
import modules.common_mod.common as common

class CrawlerCommon(common.CommonFunc):
	def __init__(self, 
			     need_stop_cheker = None, 
				 requests_delay_sec = 2,
				 request_error_pauser = None,
				 proxy = None,
				 **kwargs):
		
		super().__init__(**kwargs)

		self.need_stop_checker = need_stop_cheker
		self.requests_pauser = pauser.IntervalPauser(delay_seconds = requests_delay_sec)
		self.request_error_pauser = request_error_pauser
		self.request_tries = 5
		self.scrape_result = None
		if proxy is None:
			self.proxy = None
		else:
			self.proxy = proxy

	def check_user_interrupt(self):
		if self.need_stop_checker is None:
			return False
		self.need_stop_checker.need_stop()

	def request(self, connect_func, url, *args, **kwargs):
		request_attempt = self.request_tries
		while True:
			try:
				if request_attempt < self.request_tries:
					res = connect_func.connect() #reconnect needed
				result = connect_func(*args, **kwargs)
				if self.request_error_pauser is not None:
					self.request_error_pauser.reset()
				yield result
				break
			except ConnectionError:
				request_attempt -= 1
				if request_attempt == 0:
					raise
				else:
					#self.check_user_interrupt()
					if self.scrape_result is not None:
						self.scrape_result.add_noncritical_error(const.ERROR_CONNECTION, description = url)
					yield None
			except:
				raise
		return

class Telegram(CrawlerCommon):
	def __init__(self, username, api_id, api_hash, **kwargs):
		super().__init__(**kwargs)

		self.username = username
		self.api_id = api_id
		self.api_hash = api_hash

		self._url = r'https://t.me/'

		self.session_file_name = const.TOKEN_FOLDER + 'tg_session_'+self.username+'.txt'
		
		self.channel_parameters = {'broadcast' :True, #set true by default to crawling comments
							       'has_link'  :True, #set true by default to crawling comments
								   'megagroup' :False,
								   'supergroup':False,
								   'fake'      :False,
								   'has_geo'   :False
								  }

	def connect(self):
		#session, its_new_session = self.get_session()  #for cache in string
		
		session = const.TOKEN_FOLDER + self.username + SQLiteSession_EXTENSION #for cache in sqlite3
		#its_new_session = False
		
		if self.proxy is None:
			proxy = None
		else:
			proxy = self.proxy.get_dict_socks5()

		self.client = TelegramClient(session = session, api_id = self.api_id, api_hash = self.api_hash, proxy = proxy)
		try:
			self.client.start()
		except Exception as e:
			try:
				if not self.proxy is None:
					self.proxy.check_ip()
					e.extra_info = str(self.proxy)
			except:
				pass
			raise
		pass
		
		#if its_new_session:
		#	self.save_session()

	def close_session(self):
		self.client.session.close()

	def save_session(self):
		''' save StringSession to file '''
		with open(self.session_file_name, 'w') as f:
			psw = f.write(self.client.session.save())
			self.debug_msg('Session file saved: '+self.session_file_name)

	def get_session(self):
		''' get StringSession from file '''
		if os.path.isfile(self.session_file_name):
			with open(self.session_file_name, 'r') as f:
				session_str = f.read()
				if len(session_str) > 0 and not session_str.isspace():
					its_new_session = False
					return StringSession(session_str), its_new_session
		
		its_new_session = True
		return StringSession(), its_new_session

	def get_peer_entity(self, id = None, name_group = None, hash = None):
		if id is not None and hash is not None and type(hash) == str and hash != '':
			peer = InputPeerChannel(self.id_group, int(hash))
		elif id is not None and str(id).isdigit():
			peer = self.client.get_entity(id)
		elif name_group is not None:
			peer = self.client.get_entity(name_group)
		else:
			peer = None
		return peer

	@staticmethod
	def key_generate_only():
		import modules.crawling.accounts as accounts

		try: 
			msg_func = plpy.notice
		except:
			msg_func = print

		tg_crawler = Telegram(**accounts.TG_ACCOUNT[0], msg_func = msg_func, debug_mode = True)
		tg_crawler.connect()




#Session Files https://github.com/LonamiWebs/Telethon/blob/master/readthedocs/concepts/sessions.rst

class TelegramChannelsCrawler(Telegram):

	def __init__(self, search_keys = [], **kwargs):
		super().__init__(**kwargs)

		self.scrape_result = ScrapeResultTG(**kwargs)

		self.id_cash = []

		self.search_keys = crawler.SearchKeysGenerator(search_keys = search_keys, **kwargs)
		self.search_keys.generate_double_sequence()

		self.config_parser = common.ConfigParserNoSection()

	def crawling(self):

		for search_key in self.search_keys.search_keys_iter():
			#for step_result in self._crawling(search_key): #DEBUG
			#	yield step_result
			#return

			try:
				for step_result in self._crawling(search_key):
					yield step_result
			except exceptions.UserInterruptByDB as e:
				self.scrape_result.add_critical_error(self, expt = e, stop_process = True)
				yield self.scrape_result.to_json()  
				return
			except Exception as e:
				self.scrape_result.add_critical_error(self, expt = e, stop_process = False)
				yield self.scrape_result.to_json()  
				return 

		yield self.scrape_result.to_json()
		return

	def _crawling(self, search_key):

		self.check_user_interrupt()
		self.requests_pauser.smart_sleep()

		self.debug_msg('Search key: '+search_key)

		#result = self.client(SearchRequest(q=search_key,limit=100))
		for request_result in self.request(self.client, '', SearchRequest(q=search_key,limit=100)):
			if request_result is None: # 'None' = Connection error
				yield self.scrape_result.to_json()
			search_result = request_result

		for channel in search_result.to_dict()['chats']:
			_id = str(channel['id'])
			if not _id in self.id_cash:
				self.id_cash.append(_id)
				self.scrape_result.add_type_ACCOUNT(account_id   = _id,
													account_name = channel['username'],
													account_screen_name = channel['title'],
													account_closed = False,
													num_subscribers = channel['participants_count'],
													account_extra_1 = str(channel['access_hash']),
													parameters = self.get_channel_parameters(channel)
													)

		yield self.scrape_result.to_json()
		return		

	def direct_add_group_light(self, name_group):
		'''add group throw the search api method'''
		for search_res_in_json in self._crawling(name_group):
			break
		_scrape_result = json.loads(search_res_in_json)
		if len(_scrape_result) > 0:
			for i in range(len(_scrape_result)-1,0,-1):
				if _scrape_result[i]['account_name'] != name_group:
					_scrape_result.pop(i)

		return json.dumps(_scrape_result)

	def direct_add_group(self, name_group, only_):
		'''add group throw the direct get-peer api method. it can't be used too often'''
		peer = self.get_peer_entity(name_group = name_group)
		self.scrape_result.add_type_ACCOUNT(account_id   = str(peer.id),
											account_name = peer.username,
											account_screen_name = peer.title,
											account_closed = False,
											num_subscribers = 999999,
											account_extra_1 = str(peer.access_hash),
											parameters = self.get_channel_parameters(peer)
											)  

		return self.scrape_result.to_json()

	def get_channel_parameters(self, channel):
		par_dict = {}
		for key in self.channel_parameters:
			if isinstance(channel, dict):
				if key in channel:
					par_dict[key] = channel[key]
			elif hasattr(channel, key):
				par_dict[key] = getattr(channel, key)
		
		return self.config_parser.get_parameters_str(par_dict)

class TelegramMessagesCrawler(Telegram):

	def __init__(self, id_group, name_group, hash_group, date_deep, parameters = '', sn_recrawler_checker = None, debug_id_post = '', **kwargs):
		super().__init__(**kwargs)
		self.id_group = id_group
		self.name_group = name_group
		self.hash_group = hash_group
		self.parameters = self.channel_params_to_dict(parameters)

		self.date_deep = date.date_local_tz(date_deep)

		self.scrape_result = ScrapeResultTG(**kwargs)

		self.activity_registrator = ActivityRegistrator()

		if sn_recrawler_checker is None:
			self._sn_recrawler_checker = crawler.SnRecrawlerCheker() 
		else:
			self._sn_recrawler_checker = sn_recrawler_checker

		self.debug_id_post = str(debug_id_post)
		self.debug_id_post_processed = False

		self.have_comments = self.parameters['broadcast'] and self.parameters['has_link']

	def channel_params_to_dict(self, parameters_str):
		config_parser = common.ConfigParserNoSection()
		config_parser.read_string(parameters_str)
		loaded_pars = config_parser.keys
		for key in self.channel_parameters:
			if not key in loaded_pars:
				loaded_pars[key] = self.channel_parameters[key]
		return loaded_pars

	def crawling(self, id_group):

		if str(id_group).isdigit():
			self.id_group = int(id_group)
		else:
			self.id_group = ''
		#for step_result in self._crawling(req_group_params, id_group): #DEBUG
		#	yield step_result
		#return

		try:
			self.requests_pauser.smart_sleep()
			entity = self.get_peer_entity(self.id_group, self.name_group, self.hash_group)

			req_group_params = { 
				#'peer': self._url + self.name_group, 
				'peer': entity, 
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
		except exceptions.UserInterruptByDB as e:
			self.scrape_result.add_critical_error(self, expt = e, stop_process = True, url = self.get_group_url())
			yield self.scrape_result.to_json()  
			return
		except ChannelPrivateError as e:
			self.scrape_result.add_warning('Channel is private. id = '+ str(self.id_group) + ' name = ' + self.name_group) #TODO update field is_closed to True in sn_accounts table
			self.scrape_result.add_finish_not_found(url = id_group)
			yield self.scrape_result.to_json()  
			return
		except UsernameInvalidError as e:
			self.scrape_result.add_warning('Channel name is broken. id = '+ str(self.id_group) + ' name = ' + self.name_group) #TODO update field is_broken to True in sn_accounts table
			self.scrape_result.add_finish_not_found(url = id_group)  
			yield self.scrape_result.to_json()  
			return
		except Exception as e:
			if isinstance(e, ValueError) and isinstance(e.__cause__, UsernameNotOccupiedError):
				self.scrape_result.add_finish_not_found(url = self.get_group_url())
			elif 'Cannot find any entity corresponding to' in str(e) \
			  or 'Could not find the input entity for' in str(e):
				self.scrape_result.add_finish_not_found(url = self.get_group_url())
			else:
				self.scrape_result.add_critical_error(self, expt = e, stop_process = False, url = self.get_group_url())
			yield self.scrape_result.to_json()  
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
			self.debug_msg('ID group = '+str(self.id_group)+'	ID message = '+str(message.id)+
				           '	'+crawler.remove_empty_symbols(crawler.RemoveEmojiSymbols(message.message)))
			self.debug_msg(self.get_message_url(message.id))
			self.debug_msg(str(message.date))
			self.debug_msg(str(message.replies))

		self.activity_registrator.initialize(self.id_group)
		req_reply_params = req_message_params.copy()
		if self.debug_mode and self.debug_id_post != '':
			req_message_params['offset_id'] = int(self.debug_id_post)+1
		else:
			req_message_params['offset_id'] = 0

		self.stop_by_date_deep = False
		crawled_post_encounter = False

		self.request_counter = 1								#DEBUG
		while True:
			debug_msg_local_1()									#DEBUG

			self.check_user_interrupt()
			self.requests_pauser.smart_sleep()
			#history_posts = self.client(GetHistoryRequest(**req_message_params))
			for request_result in self.request(self.client, self.get_group_url(), GetHistoryRequest(**req_message_params)):
				if request_result is None: # 'None' = Connection error
					yield self.scrape_result.to_json()
				history_posts = request_result
			
			if not history_posts.messages:
				break

			req_message_params['offset_id'] = history_posts.messages[-1].id #for next request
			for message in history_posts.messages:
				if message.message is None:
					continue
				if self.check_debug_post_id(message.id):
					continue
				self.check_date_deep(message.date)
				if self.stop_by_date_deep:
					break
				if self._sn_recrawler_checker.is_crawled_post(message.date):
					crawled_post_encounter = True
					break

				debug_msg_local_2()									#DEBUG

				self.scrape_result.add_message(message, self.get_message_url(message.id),  self.id_group)
				#_message.sender_id
				#_message.views
				
				if self.have_comments:
					self.activity_registrator.registrate(message.id, message.date)
				else:
					self.activity_registrator.registrate_common_date(message.date)

				#Replyes
				if self.have_comments:
					if message.replies is not None and message.replies.replies > 0:
						for _ in self._crawling_replies(req_reply_params, self.id_group, message.id):
							pass

			if crawled_post_encounter:
				if self.have_comments:
					for post_for_recrawl_reply in self._sn_recrawler_checker.get_post_list():
						if self.check_debug_post_id(post_for_recrawl_reply):
							continue
						id_message = int(post_for_recrawl_reply)
						message = self.client.get_messages(req_message_params['peer'], ids=id_message)
						if message is not None and message.replies is not None and message.replies.replies > 0:
							for _ in self._crawling_replies(req_reply_params, self.id_group, id_message):
								pass

			self.activity_registrator.move_to_scrape_result(self.scrape_result, move_common_date = self.debug_id_post == '')		
			yield self.scrape_result.to_json() #return result per one post-request
			if self.stop_by_date_deep:
				self.debug_msg('STOP BY DEEP DATE	STOP BY DEEP DATE	STOP BY DEEP DATE	STOP BY DEEP DATE')
				break
			if crawled_post_encounter:
				self.debug_msg('STOP BY CRAWLED POST ENCOUNTER')
				break
			if self.debug_id_post != '' and self.debug_id_post_processed:
				break

		self.scrape_result.add_finish_success(self.get_group_url())
		yield self.scrape_result.to_json()
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
			nonlocal reply, id_message
			self.debug_msg('________________REPLY REPLY___________________________')
			self.debug_msg('ID group = '+str(self.id_group)+'	ID message = '+str(id_message)+
						   '	ID reply = '+str(reply.id)+'	Reply to ID = ' + str(reply.reply_to_msg_id) + 
						   '	' + crawler.remove_empty_symbols(crawler.RemoveEmojiSymbols(reply.message)))
			self.debug_msg(self.get_reply_url(reply.id, id_message))
			self.debug_msg(str(reply.date))
			self.debug_msg(str(reply.replies))

		
		first_request = True
		crawled_reply_encounter = False
		req_reply_params['msg_id'] = id_message
		req_reply_params['offset_id'] = 0
		offset_reply = 0

		while True:
			self.check_user_interrupt()
			self.requests_pauser.smart_sleep()

			debug_msg_local_1()									#DEBUG

			try: 
				#history_repl = self.client(GetRepliesRequest(**req_reply_params))
				for request_result in self.request(self.client, self.get_group_url(), GetRepliesRequest(**req_reply_params)):
					if request_result is None: # 'None' = Connection error
						yield self.scrape_result.to_json()
					history_repl = request_result
			except MsgIdInvalidError as e:
				self.debug_msg('ERROR INVALID ID. Group = ' + self.get_group_url() + '	Message ID = '+self.get_message_url(id_message)) #TODO
				#ignoring, perhaps it was a temporary promotional message 
				break
			except Exception as e:
				raise

			if not history_repl.messages:
				break

			for reply in history_repl.messages:
				if self._sn_recrawler_checker.is_reply_out_of_upd_date(str(id_message), reply.date):
					crawled_reply_encounter = True
					break

				debug_msg_local_2()								#DEBUG


				#reply.reply_to_msg_id - this field contains the id of the reply to which the response is being sent #TODO
				self.scrape_result.add_reply(reply, self.get_reply_url(reply.id, id_message),  id_group, id_message)
				if first_request:
					if self.have_comments:
						self.activity_registrator.registrate(id_message, reply.date)
					else:
						self.activity_registrator.registrate_common_date(reply.date)
					first_request = False
			
			req_reply_params['offset_id'] = history_repl.messages[-1].id

			if crawled_reply_encounter:
				break
		return

	def get_group_url(self):
		return self._url+str(self.name_group)

	def get_message_url(self, id_message):
		return self.get_group_url()+'/'+str(id_message)

	def get_reply_url(self, id_reply, id_message):
		return self.get_message_url(id_message) + '?comment=' + str(id_reply)

	def check_date_deep(self, dt):
		if self.date_deep != const.EMPTY_DATE and dt <= self.date_deep:
			self.stop_by_date_deep = True

	def check_debug_post_id(self, message_id):
		need_skip = True
		if not self.debug_mode:
			return not need_skip
		if self.debug_id_post == '':
			return not need_skip
		if self.debug_id_post != str(message_id):
			return need_skip
		self.debug_id_post_processed = True
		return not need_skip

class ScrapeResultTG(scraper.ScrapeResult):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def add_message(self, message, url, group_id):

		super().add_type_POST(
			url = url,
			#sn_id = group_id,
			sn_id = group_id,#[0:12], #DEBUG  CROP ID !!!
			sn_post_id = str(message.id),
			author = str(message.sender_id),
			content_date = date.date_to_str(message.date),
			content = message.message
			)		
	
	def add_reply(self, reply, url, group_id, message_id):

		super().add_type_REPLY(
			url = url,
			#sn_id = group_id,
			sn_id = group_id,#[0:12], #DEBUG CROP ID
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

	def registrate_common_date(self, dt):
		if dt is None or dt == self.EMPTY_DATE: 
			return

		if self.common_last_date < dt:
			self.common_last_date = dt

	def move_to_scrape_result(self, scrape_result, move_common_date = True):
		for sn_post_id in self:
			scrape_result.add_type_activity(self.sn_id, str(sn_post_id), self[sn_post_id])  #DEBUG  CROP ID !!![0:12]
		if move_common_date and self.common_last_date != self.EMPTY_DATE:
			scrape_result.add_type_activity(self.sn_id, '', self.common_last_date)				#DEBUG  CROP ID !!![0:12]
		self.clear()