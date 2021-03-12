from telethon.sync import TelegramClient

client = TelegramClient(session = 'tg_api_key', api_id = '3756968', api_hash = '6f685fcaf0056c27f8b5d77ef1265f04')

client.start()

