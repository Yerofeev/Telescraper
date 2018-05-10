import multiprocessing as mp
import threading
from time import sleep, time
import queue
import sys
import os
import datetime
import re
from itertools import repeat
from functools import partial

from telethon import TelegramClient
from telethon import errors
from telethon.tl.functions.messages import GetHistoryRequest
import mysql.connector

from te_thread import time_correction
from credentials import identity, config
from miscellaneous import Logger

sys.stdout = Logger(os.path.basename(__file__))
print(os.path.basename(__file__))
print(datetime.datetime.now().time())


def get_date(cursor, channel_name, lock):
	with lock:
		cursor.execute("select  channel_name,date,time from messages where channel_name LIKE '%s' order by date desc, time desc limit 1;" % channel_name)
	row = cursor.fetchone()
	if row:
		print("FETCH: ", end='')
		# transform to datetime format
		date_ = datetime.datetime.combine(row[1], datetime.time((row[2].seconds // 3600), (row[2].seconds // 60) % 60))
		# UFC time  -3 hours : -2h59m not to fetch already downloaded messages
		date_ += datetime.timedelta(hours=0, minutes=1)
		#date_ = datetime.datetime(2018, 1, 1, 00, 00, 00)#DELETE
	else:
		print("-",end='')
		date_ = datetime.datetime(2018, 1, 1, 00, 00, 00)
	return date_


def chunks_threads(li, n):
	"""Divide list in n parts."""
	index = int(len(li) / n + 0.5)
	for i in range(n-1):
		yield li[i*index:i*index + index]
	yield li[n*index - index:]



def establish_connection(api_id, api_hash, session):
	client = TelegramClient(session, api_id, api_hash)
	retries = 5
	while retries > 0:
		if client.connect():
			print('Client connected')
			break
		retries -= 1
	else:
		print("Cannot connect!")
		return None
	if client.is_user_authorized() is False:
		print('Not authorized')
	else:
		print('AUTHORIZED')
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
	if hasattr(channel, 'channel_id'):
		return channel, channel.channel_id
	else:
		return None, None
	
	
def get_tel_history(client, channel, channel_name, limit=100, offset_id=0, offset_date=None, x=0):
	i = 0
	while True:
		try:
			m = client(GetHistoryRequest(peer=channel, limit=limit, offset_date=None, offset_id=offset_id, min_id=0, max_id=0, add_offset=0, hash=0))
			break
		except RuntimeError as e:
			i += 1
			m = 0
			print(e, 'wait...')
			sleep(30)
		except errors.ChannelPrivateError as e:
			i += 1
			m = 0
			print(e, channel_name)
			break
		except Exception as e:
			print("Undefined error!", channel_name, identity['mock_file'][x], e)
			i += 1
			m = 0
			break
	return m, i


def message_download(client, channel, channel_name, channel_id, x, cursor, lock):
	offset_id = 0
	pid_thread = 'Process #' + str(x + 1) + ' ' + str(os.getpid())
	print(pid_thread)
	date_ = get_date(cursor, channel_name, lock )
	#date_ = datetime.datetime(2018, 1, 1, 00, 00, 00)
	print(date_)
	while True:
		s = time()
		m, failed = get_tel_history(client, channel, channel_name, offset_id=offset_id, x=x)
		print('get_history time for', channel_name, time() - s, sep=' ')
		if failed and not hasattr(m, 'messages'):
			return
		# -----------------------------------Attributes handling-------------------------------------------------------#
		for msg in reversed(m.messages):
			if msg.date < date_:
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
			# ---------------------------------Put to queue ------------------------------------------------------#
			q.put(li)
			# ---------------------------------Check date -------------------------------------------------------#
		if m.messages and m.messages[-1].date > date_:       # if the oldest message is newer than NY
			offset_id = m.messages[-1].id                           # get ready for the next 100 messages
			print('-----', channel_name, offset_id, m.messages[-1].date, m.messages[0].date)
			if offset_id < 0:           # new channel with fewer than 300 messages or if in the 2017
				return
		else:   # the oldest message already from 2017, return
			return


def worker(i, cursor, lock ):
	client = establish_connection(identity['api_id'][i], identity['api_hash'][i], identity['session'][i])
	with open(identity['mock_file'][i], 'r+') as f:
		for index, channel_name in enumerate(f):
			channel_name = channel_name[:-1]
			channel, channel_id = get_channel(client, channel_name)
			if channel:
				message_download(client, channel, channel_name, channel_id, i, cursor, lock)
	print('Process #' + str(i) + ' (' + str(os.getpid()) + ')' + 'exited')


def q_worker(c, cursor, count, lock):
	add_text = ("INSERT INTO messages "
				"(channel_name, channel_id, message_id,  date, time, url, views, message, date_added, time_added, PID_Thread)"
				"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
	while True:
		try:
			obj = q.get()
			if obj == 'DONE':
				print("FINISHED")
				break
			else:
				# --------------------------Writing to MySQL db--------------------------------------------------------#
				with lock:
					count.value += 1
					try:
						cursor.execute(add_text, obj)
						c.commit()
					except Exception as e:
						print('Exception in SQL', e)
						c.rollback()
		except queue.Empty as e:
			print("-", e)
			break


def main():
	count = mp.Value('i', 0)
	c = mysql.connector.connect(**config)
	cursor = c.cursor(buffered=True)
	N = 2
	processes = []
	lock = mp.Lock()
	for p_number in range(len(identity['api_id'])):
		p = mp.Process(target=worker, args=(p_number, cursor, lock))
		processes.append(p)
		p.daemon = True
		p.start()
	# p = mp.Process(target=foo_put, args=(q,)).start()
	for _ in range(5):
		pq = mp.Process(target=q_worker, args=(c, cursor, count, lock))
		pq.daemon = True
		pq.start()
	for p in processes:
		p.join()
	q.put('DONE')
	q.close()
	q.join_thread()
	print('Processed: ', count.value - 1)
	print((count.value - 1) // (int(time() - start_time)), 'messages/s')

if __name__ == '__main__':
	try:
		start_time = time()
		q = mp.Queue()
		main()
	finally:
		print('Finished in ', time() - start_time)