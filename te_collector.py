from time import sleep, time
import os
import sys
import random
import datetime

from telethon import TelegramClient
from telethon import errors

from credentials import identity, telegram_txt, not_used_names
from te_files import delete_row, tracer
from miscellaneous import Logger

sys.stdout = Logger()
print(os.path.basename(__file__))
print(datetime.datetime.now().time())


def get_channel(client, channel_name):
	try:
		channel = client.get_input_entity(channel_name)
	except (errors.UsernameNotOccupiedError,  errors.UsernameInvalidError) as e:
		print(str(e) + ': ' + channel_name)
		delete_row(channel_name, telegram_txt)
		with open(not_used_names, 'a+') as f_good:
			f_good.write(channel_name)
	except errors.FloodWaitError as e:
		print(e)
		return -1
	except errors.RPCError as e:
		print(e)
	else:
		return channel


def establish_connection(session, api_id, api_hash, i):
	client = TelegramClient(session, api_id, api_hash)
	while True:
		try:
			client.connect()
			break
		except RuntimeError as e:
			print(e)
			sleep(30)
	if not client.is_user_authorized():
		print('User#' + str(i+1) + ' is NOT authorized')
		return None
	else:
		print('User#' + str(i+1) + ' is authorized')
		return client


def main_loop(client, index):
	with open(telegram_txt, 'r') as f:
		for i, channel_name in enumerate(f):
			sleep(random.uniform(1, 1.5))
			channel = get_channel(client, channel_name)
			if channel == -1:
				return
			elif channel:
				print('user #', index+1, '   ',i, channel_name)
				# delete this channel from origins
				delete_row(channel_name, telegram_txt)
				# write this channel_name to corresponding file
				with open(identity['file'][index], 'a') as f_good:
					f_good.write(channel_name)


def main():
	try:
		for i in range(1, len(identity['session'])):
			session = identity['session'][i]
			api_id = identity['api_id'][i]
			api_hash = identity['api_hash'][i]
			client = establish_connection(session, api_id, api_hash, i)
			if client:
				main_loop(client, i)
	finally:
		print(datetime.datetime.now().time())
		print('-'*70)

if __name__ == '__main__':
		main()
