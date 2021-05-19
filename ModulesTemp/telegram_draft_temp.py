import configparser
import json

from telethon.sync import TelegramClient
from telethon import connection

# для корректного переноса времени сообщений в json
from datetime import date, datetime

# классы для работы с каналами
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

# класс для работы с сообщениями
from telethon.tl.functions.messages import GetHistoryRequest, GetRepliesRequest

import Modules.Crawling.accounts as accounts

import time

import Modules.Crawling.crawler as crawler

import typing 

class MyTelegramClient(TelegramClient):

    def start(
            self: 'TelegramClient',
            phone: typing.Callable[[], str] = lambda: input('cass Please enter your phone (or bot token): '),
            password: typing.Callable[[], str] = lambda: getpass.getpass('cass Please enter your password: '),
            *,
            bot_token: str = None,
            force_sms: bool = False,
            code_callback: typing.Callable[[], typing.Union[str, int]] = None,
            first_name: str = 'New User',
            last_name: str = '',
            max_attempts: int = 3) -> 'TelegramClient':
        """
        Starts the client (connects and logs in if necessary).

        By default, this method will be interactive (asking for
        user input if needed), and will handle 2FA if enabled too.

        If the phone doesn't belong to an existing account (and will hence
        `sign_up` for a new one),  **you are agreeing to Telegram's
        Terms of Service. This is required and your account
        will be banned otherwise.** See https://telegram.org/tos
        and https://core.telegram.org/api/terms.

        If the event loop is already running, this method returns a
        coroutine that you should await on your own code; otherwise
        the loop is ran until said coroutine completes.

        Arguments
            phone (`str` | `int` | `callable`):
                The phone (or callable without arguments to get it)
                to which the code will be sent. If a bot-token-like
                string is given, it will be used as such instead.
                The argument may be a coroutine.

            password (`str`, `callable`, optional):
                The password for 2 Factor Authentication (2FA).
                This is only required if it is enabled in your account.
                The argument may be a coroutine.

            bot_token (`str`):
                Bot Token obtained by `@BotFather <https://t.me/BotFather>`_
                to log in as a bot. Cannot be specified with ``phone`` (only
                one of either allowed).

            force_sms (`bool`, optional):
                Whether to force sending the code request as SMS.
                This only makes sense when signing in with a `phone`.

            code_callback (`callable`, optional):
                A callable that will be used to retrieve the Telegram
                login code. Defaults to `input()`.
                The argument may be a coroutine.

            first_name (`str`, optional):
                The first name to be used if signing up. This has no
                effect if the account already exists and you sign in.

            last_name (`str`, optional):
                Similar to the first name, but for the last. Optional.

            max_attempts (`int`, optional):
                How many times the code/password callback should be
                retried or switching between signing in and signing up.

        Returns
            This `TelegramClient`, so initialization
            can be chained with ``.start()``.

        Example
            .. code-block:: python

                client = TelegramClient('anon', api_id, api_hash)

                # Starting as a bot account
                await client.start(bot_token=bot_token)

                # Starting as a user account
                await client.start(phone)
                # Please enter the code you received: 12345
                # Please enter your password: *******
                # (You are now logged in)

                # Starting using a context manager (this calls start()):
                with client:
                    pass
        """
        self.session.filename = 'TG111session\Cassandra.session'

        if code_callback is None:
            def code_callback():
                return input('Please enter the code you received: ')
        elif not callable(code_callback):
            raise ValueError(
                'The code_callback parameter needs to be a callable '
                'function that returns the code you received by Telegram.'
            )

        if not phone and not bot_token:
            raise ValueError('No phone number or bot token provided.')

        if phone and bot_token and not callable(phone):
            raise ValueError('Both a phone and a bot token provided, '
                             'must only provide one of either')

        coro = self._start(
            phone=phone,
            password=password,
            bot_token=bot_token,
            force_sms=force_sms,
            code_callback=code_callback,
            first_name=first_name,
            last_name=last_name,
            max_attempts=max_attempts
        )
        return (
            coro if self.loop.is_running()
            else self.loop.run_until_complete(coro)
        )

class Telegram:
	def __init__(self, username, api_id, api_hash, 
			        msg_func = None,
					debug_mode = True,
					**kwargs
				):
		self.username = username
		self.api_id = api_id
		self.api_hash = api_hash

		self.msg_func = msg_func         # функция для сообщений

		self.debug_mode = debug_mode

	def connect(self):
		self.client = MyTelegramClient(session = self.username, api_id = self.api_id, api_hash = self.api_hash)
		self.client.start()
		
	def msg(self, message):

		if not self.msg_func is None:
			try:
				self.msg_func(message)
			except:
				pass

	def _debug_msg(self, message):
		if self.debug_mode:
			self.msg(message)

class TelegramGroupSearch(Telegram):
	def __init__(self, *args, **kwargs):
		return super().__init__(*args, **kwargs)

class TelegramCrawlMessages(Telegram):
	def __init__(self, *args, **kwargs):
		self.messages = []
		return super().__init__(*args, **kwargs)

	async def crawl_messages(self, id_group):
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

				if _message.replies is not None and _message.replies.replies > 0:
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

	async def put_to_db(self):
		self._debug_msg('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
		self._debug_msg('@@@@@@@@@@@@@  put_to_db @@@@@@@@@@@@@@@@@@')
		await self._debug_msg('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
		#await

	async def main(self, id_group):
		await self.crawl_messages(id_group)
		await self.put_to_db()

	def start(self, id_group):
		with self.client:
			self.client.loop.run_until_complete(self.main(id_group))

async def dump_all_participants(channel, all_participants):
	"""Записывает json-файл с информацией о всех участниках канала/чата"""
	offset_user = 0    # номер участника, с которого начинается считывание
	limit_user = 100   # максимальное число записей, передаваемых за один раз

	all_participants = []   # список всех участников канала
	filter_user = ChannelParticipantsSearch('')

	while True:
		participants = await client(GetParticipantsRequest(channel,
			filter_user, offset_user, limit_user, hash=0))
		if not participants.users:
			break
		all_participants.extend(participants.users)
		offset_user += len(participants.users)

	all_users_details = []   # список словарей с интересующими параметрами участников канала

	for participant in all_participants:
		all_users_details.append({"id": participant.id,
			"first_name": participant.first_name,
			"last_name": participant.last_name,
			"user": participant.username,
			"phone": participant.phone,
			"is_bot": participant.bot})

	with open('channel_users.json', 'w', encoding='utf8') as outfile:
		json.dump(all_users_details, outfile, ensure_ascii=False)


async def dump_all_messages(channel, all_messages):
	"""Записывает json-файл с информацией о всех сообщениях канала/чата"""
	offset_msg = 0    # номер записи, с которой начинается считывание
	limit_msg = 100   # максимальное число записей, передаваемых за один раз

	#all_messages = []   # список всех сообщений
	total_messages = 0
	total_count_limit = 10  # поменяйте это значение, если вам нужны не все сообщения

	class DateTimeEncoder(json.JSONEncoder):
		'''Класс для сериализации записи дат в JSON'''
		def default(self, o):
			if isinstance(o, datetime):
				return o.isoformat()
			if isinstance(o, bytes):
				return list(o)
			return json.JSONEncoder.default(self, o)

	while True:
		history = await client(GetHistoryRequest(
			peer=channel,
			offset_id=offset_msg,
			offset_date=None, add_offset=0,
			limit=limit_msg, max_id=0, min_id=0,
			hash=0))
		if not history.messages:
			break
		messages = history.messages
		for message in messages:
			all_messages.append(message.to_dict())
		offset_msg = messages[len(messages) - 1].id
		total_messages = len(all_messages)
		if total_count_limit != 0 and total_messages >= total_count_limit:
			break

	with open('channel_messages.json', 'w', encoding='utf8') as outfile:
		 json.dump(all_messages, outfile, ensure_ascii=False, cls=DateTimeEncoder)


async def main():
	channel = r'https://t.me/Chelabinsk_tut'
	all_messages = []

	#await dump_all_participants(channel)
	await dump_all_messages(channel, all_messages)
	pass


if __name__ == "__main__" or True:

	#with open('TextFile5.txt',encoding = 'UTF-8') as f:
	#	t = f.read()
	#	t = crawler.RemoveEmojiSymbols(t)
	#	t = crawler.remove_empty_symbols(t)
	#	print(t)

	#raise
	tg_crawler = TelegramCrawlMessages(**accounts.TG_ACCOUNT[0], msg_func = print)
	tg_crawler.connect()
	#crawler.crawl_messages('Chelabinsk_tut')
	#crawler.crawl_messages('andrey_fursov')
	#tg_crawler.crawl_messages('govoritfursov')
	tg_crawler.start('govoritfursov')

	#api_id   = accounts.TG_ACCOUNT[0]['api_id']
	#api_hash = accounts.TG_ACCOUNT[0]['api_hash']
	#username = accounts.TG_ACCOUNT[0]['username']
	#phone    = accounts.TG_ACCOUNT[0]['phone']
	#client = TelegramClient(session = username, api_id = api_id, api_hash = api_hash)

	#client.start()

	#crawler = TelegramCrawlMessages(**accounts.TG_ACCOUNT[0])
	pass

#with client:
#	client.loop.run_until_complete(main())
pass