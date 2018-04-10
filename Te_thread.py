from time import sleep, timezone, time
import datetime
import os
import sys
import collections
import re
import threading

from telethon import TelegramClient
from telethon import errors
from telethon.tl.functions.messages import GetHistoryRequest
import mysql.connector

from credentials import identity, config
from collections import defaultdict
from miscellaneous import Logger




def chunks_threads(li, n):
	"""Divide list in n parts."""
	index = int(len(li) / n + 0.5)
	for i in range(n-1):
		yield li[i*index:i*index + index]
	yield li[n*index - index:]


def chunks(li, n):
	"""Yield successive n-sized chunks from l."""
	for i in range(0, len(li), n):
		yield li[i:i + n]


def stat_channels(channel_name, i):
	with open(identity['problems'][i], 'a') as file:
		print('problems')
		file.write(channel_name+'\n')


def time_correction(date):
	time_offset = timezone / 3600
	t = str(date - datetime.timedelta(hours=time_offset))
	return t[:10], t[11:]


def establish_connection(api_id, api_hash, session):
	client = TelegramClient(session, api_id, api_hash)
	client.connect()
	if client.is_user_authorized() is False:
		print('Not authorized')
		sys.exit()
	return client


def get_channel(client, channel_name):
	try:
		channel = client.get_input_entity(channel_name)
	except errors.UsernameNotOccupiedError as e:
		print(e)
		return None, None
	except RuntimeError as e:
		print(e)
		return None, None
	except errors.FloodWaitError as e:
		print(e, channel_name)
		return None, None
	except:
		return None, None
	if hasattr(channel, 'channel_id'):
		return channel, channel.channel_id
	else:
		return None, None


def get_tel_history(client, channel, channel_name, x, limit=10, offset_id=0, offset_date=None):
	i = 0
	while True:
		try:
			m = client(GetHistoryRequest(peer=channel, limit=limit, offset_date=None, offset_id=offset_id, min_id=0, max_id=0, add_offset=0, hash=0))
			break
		except RuntimeError as e:
			i += 1
			m = 0
			print(e, 'wait...')
			sleep(60+x)
		except errors.ChannelPrivateError as e:
			i += 1
			m = 0
			print(e, channel_name)
			break
		except Exception as e:
			print("Undefined error!",e)
			i += 1
			m = 0
			break
	return m, i


def message_download(client, channel, channel_name, channel_id, c, cursor, lock, x, date_dict):
	global count
	add_text = ("INSERT INTO messages "
				"(channel_name, channel_id, message_id,  date, time, url, views, message, date_added, time_added, PID_Thread)"
				"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
	offset_id = 0
	limit = 100
	result = 0
	pid_thread = 'Process ' + str(os.getpid()) + '/Thread #' + str(x + 1)
	try:
		date_dict[channel_name]
	except KeyError:
		date_dict[channel_name] = datetime.datetime(2018, 1, 1, 00, 00, 00)
	date_dict[channel_name] = datetime.datetime(2018, 1, 1, 00, 00, 00)
	print('date: ', date_dict[channel_name])
	while True:
		s = time()
		m, i = get_tel_history(client, channel, channel_name, x, limit=limit, offset_id=offset_id)
		print('get_history time', time() - s)
		s = time()
		if i != 0 and not hasattr(m, 'messages'):
			result += 1
			return result
		if m.messages and m.messages[0].date < date_dict[channel_name]:
			print(channel_name, 'no messages since', date_dict[channel_name] )
			result += 1
			return result
		# -----------------------------------Attributes handling-------------------------------------------------------#
		for msg in reversed(m.messages):
			if msg.date < date_dict[channel_name]:
				continue
			try:
				views = msg.views
			except AttributeError:
				views = 0
			try:
				text = re.sub(r'[^\w\s,]', '', msg.message)
			except AttributeError:
				text = ''
			correct_date, correct_time = time_correction(msg.date)
			url = 'https://t.me/' + channel_name[1:-1] + '/' + str(msg.id)
			li = [
				channel_name,
				channel_id,
				msg.id,
				correct_date,
				correct_time,
				url,
				views,
				text,
				datetime.datetime.now().today(),
				datetime.datetime.now().time(),
				pid_thread,
			]
			print('elapsed time1: ', time() - s)
			s = time()
			# --------------------------Writing to MySQL db------------------------------------------------------------#
			with lock:
				count += 1
				try:
					cursor.execute(add_text, li)
					c.commit()
				except Exception as e:
					print('Exception in SQL', e)
					c.rollback()
			print('elapsed time sql: ', time() - s)
			result = 1
			# only if not the 1st time: if there is message from NY than break  ---------------------------------------#
		if m.messages and m.messages[-1].date > date_dict[channel_name]:       # if the oldest message is newer than NY
			offset_id = m.messages[-1].id                           # get ready for the next 100 messages
			print('----', channel_name, offset_id, m.messages[-1].date, m.messages[0].date)
			if offset_id < 0:           # new channel with fewer than 300 messages or if in the 2017
				return result
		else:   # the oldest message already from 2017, return
			return result


def func(client, li, x, c, cursor, lock, iteration, date_dict):
	sleep(x)
	number_of_retries = 2
	# if empty list because number of threads > than chunks
	if not li:
		return
	try:
		for j in range(number_of_retries):
			for i, channel_name in enumerate(li):
				if channel_name != 1:
					channel, channel_id = get_channel(client, channel_name)
					print("Thread's #", x, i, channel_name, sep='.')
					sleep(1)
					if channel:
						result = message_download(client, channel, channel_name, channel_id, c, cursor, lock, x, date_dict)
						if result != 0:
							li[i] = 1
			if li.count(1) == len(li):
				break
			print("Thread's #", iteration, x, 'Number of 1:', li.count(1), "/", len(li), sep='.')
	finally:
		if li.count(1) != len(li):
			with lock:
				for i in li:
					if i != 1:
						stat_channels(i, iteration)
		print("Thread's #", iteration, x,  'Finished', li.count(1), "/", len(li), sep='.')


def main():
	number_threads = 10
	print(datetime.datetime.now().time())
	c = mysql.connector.connect(**config)
	cursor = c.cursor(buffered=True)
	#date_dict = defaultdict(lambda: datetime.datetime.now(), key="some_value")
	date_dict = {}
	cursor.execute(
		"select s.channel_name, s.date, max(s.time) from(select mmm.channel_name, mmm.date,  mmm.time from messages mmm inner join(select distinct channel_name, max(date) as MD from messages group by channel_name) m on mmm.channel_name = m.channel_name and mmm.date = m.MD) s group by channel_name;")
	rows = cursor.fetchall()
	for i in rows:
		date_ = datetime.datetime.combine(i[1], datetime.time((i[2].seconds // 3600), (i[2].seconds // 60) % 60))
		# UFC time  -3 hours : -2h59m not to fetch already downloaded messages
		date_ -= datetime.timedelta(hours=2, minutes=59)
		date_dict[i[0]] = date_
	print(date_dict)
	start = time()
	try:
		for i in range(2,5):#(len(identity)):
			li = []
			fn = identity['file'][i]
			with open(fn, "r") as f:#simplify with readlines
				for line in f:
					li.append(line[:-1])
			client = establish_connection(identity['api_id'][i], identity['api_hash'][i], identity['session'][i])
			threads = []
			lock = threading.Lock()
			for chunk_list in (list(chunks(li, 100))):
				li_x = list(chunks_threads(chunk_list, number_threads))
				for x in range(number_threads):
					thread = threading.Thread(target=func, args=(client, li_x[x], x, c, cursor, lock, i, date_dict))
					thread.daemon = True
					threads.append(thread)
					thread.start()
				for thread in threads:
					thread.join()
				print('++++++++++++++++++sssssss++++++++++++++++++')
		print('=============SSS==================')
	finally:
		cursor.close()
		c.close()
		client.disconnect()
		print(count)
		print('Done in: ', time() - start)


if __name__ == '__main__':
	count = 0
	sys.stdout = Logger()
	print(os.path.basename(__file__))
	print(datetime.datetime.now().time())
	main()
