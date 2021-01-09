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
from telethon.tl.functions.messages import GetHistoryRequest

import CrawlModulesPG.accounts as accounts


class Telegram:
	def __init__(self, username, api_id, api_hash, 
			        msg_func = None,
					**kwargs
				):
		self.username = username
		self.api_id = api_id
		self.api_hash = api_hash

		self.msg_func = msg_func         # функция для сообщений

	def connect(self):
		self.client = TelegramClient(session = self.username, api_id = self.api_id, api_hash = self.api_hash)
		self.client.start()
		
	def msg(self, message):

		if not self.msg_func == None:
			try:
				self.msg_func(message)
			except:
				pass

class TelegramGroupSearch(Telegram):
	def __init__(self, *args, **kwargs):
		return super().__init__(*args, **kwargs)

class TelegramCrawlMessages(Telegram):
	def __init__(self, *args, **kwargs):
		self.messages = []
		return super().__init__(*args, **kwargs)

	def crawl_messages(self, id_group):
		_channel = r'https://t.me/' + id_group
		_offset_msg = 0
		_limit_msg = 100

		while True:
			_history = self.client(GetHistoryRequest(
				peer=_channel,
				offset_id=_offset_msg,
				offset_date=None, add_offset=0,
				limit=_limit_msg, max_id=0, min_id=0,
				hash=0))
			if not _history.messages:
				break
			for _message in _history.messages:
				self.messages.append(_message.to_dict())

				self.msg('___________________________________________')
				self.msg(_message.message)
				self.msg(str(_message.date))
				self.msg(str(_message.replies))

			_offset_msg = _history.messages[len(_history.messages) - 1].id
			
			#DEBUG
			_total_messages = len(_history.messages)
			if _total_messages >= 300:
				break
		pass

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


if __name__ == "__main__":
	crawler = TelegramCrawlMessages(**accounts.TG_ACCOUNT[0], msg_func = print)
	crawler.connect()
	crawler.crawl_messages('Chelabinsk_tut')

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