from telethon import TelegramClient
from time import sleep, timezone, time
from csv import writer
from telethon.tl.types import PeerChannel
from telethon.tl.functions.channels import GetFullChannelRequest
from datetime import datetime, timedelta
import os
import sys
import mysql.connector
import collections
import re
import threading
from telethon import errors
from telethon.tl.functions.messages import GetHistoryRequest
from credentials import identity, password

def chunks_threads(li, n):
    """Divide list in n parts."""
    for i in range(n):
        index = len(li)*i // n
        if i == (n - 1):
            yield li[index:]
        else:
            yield li[index:index + len(li) // n]


def chunks(li, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(li), n):
        yield li[i:i + n]


def stat_channels(channel_name, filename):
    with open(filename, 'a') as file:
        print('problems')
        file.write(channel_name+'\n')


def time_correction(date):
    time_offset = timezone / 3600
    t = str(date-timedelta(hours=time_offset))
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
        print(e)
        return None, None
    except:
        return None, None
    if hasattr(channel, 'channel_id'):
        return channel, channel.channel_id
    else:
        return None, None


def get_tel_history(client, channel, channel_name, x, limit=100, offset_id=0):
    i = 0
    try:
        m = client(GetHistoryRequest(peer=channel, limit=limit, offset_date=None, offset_id=offset_id,
                                     min_id=0, max_id=0, add_offset=0, hash=0))
    except errors.ChannelPrivateError as e:
        i += 1
        m = 0
        print(e, channel_name)
    except RuntimeError as e:
        i += 1
        m = 0
        print(e, 'wait...')
        sleep(60+x)
    except:
        i += 1
        m = 0
    return m, i


def message_download(client, channel, channel_name, channel_id, c, cursor, lock, x):
    add_text = ("INSERT INTO messages "
                "(channel_name, channel_id, message_id,  date, time, url, views, message, date_added, time_added, PID_Thread)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    date_2018 = datetime(2018, 1, 1, 00, 00, 00)
    offset_id = 0
    limit = 100
    result = 0
    while True:
        s = time()
        m, i = get_tel_history(client, channel, channel_name, x, limit=limit, offset_id=offset_id)
        print('get_history time', time() - s)
        if i != 0 and not hasattr(m, 'messages'):
            result += 1
            return result
        if m.messages and m.messages[0].date < date_2018:
            print(channel_name, 'no messages from 2018')
            result += 1
            return result
        # -----------------------------------Attributes handling-------------------------------------------------------#
        for msg in reversed(m.messages):
            if msg.date < date_2018:
                # print(len(m.messages), msg.id, msg.date)
                continue
            if hasattr(msg, 'views'):
                views = msg.views
            else:
                views = 0
            if hasattr(msg, 'message') and msg.message:
                text = re.sub(r'[^\w\s,]', '', msg.message)
            else:
                text = ''
            correct_date, correct_time = time_correction(msg.date)
            url = 'https://t.me/' + channel_name[1:-1] + '/' + str(msg.id)
            pid_thread = 'Process ' + str(os.getpid()) + '/Thread #' + str(x + 1)
            li = [
                channel_name[:-1],
                channel_id,
                msg.id,
                correct_date,
                correct_time,
                url,
                views,
                text,
                datetime.now().today(),
                datetime.now().time(),
                pid_thread,
            ]
            # --------------------------Writing to MySQL db------------------------------------------------------------#
            with lock:
                cursor.execute(add_text, li)
                c.commit()
            result = 1
            # only if not the 1st time: if there is message from NY than break  ---------------------------------------#
        if m.messages and m.messages[-1].date > date_2018:       # if the oldest message is newer than NY
            offset_id = m.messages[-1].id                           # get ready for the next 100 messages
            print('----', channel_name[:-1], offset_id, m.messages[-1].date, m.messages[0].date)
            if offset_id < 0:           # new channel with fewer than 300 messages or if in the 2017
                return result
        else:   # the oldest message already from 2017, return
            return result


def func(client, li, x, c, cursor, lock, fn):
    sleep(x)
    number_of_retries = 2
    try:
        for j in range(number_of_retries):
            for i, channel_name in enumerate(li):
                if channel_name != 1:
                    channel, channel_id = get_channel(client, channel_name)
                    print("Thread's #", fn[21], x, i, channel_name, sep='.')
                    if i > 0 and i % 15 == 0:
                        sleep(10)
                    if channel:
                        result = message_download(client, channel, channel_name, channel_id, c, cursor, lock, x)
                        if result != 0:
                            li[i] = 1
            if li.count(1) == len(li):
                break
            print("Thread's #", fn[21], x, 'Number of 1:', li.count(1), "/", len(li), sep='.')
    finally:
        if li.count(1) != len(li):
            with lock:
                for i in li:
                    if i != 1:
                        stat_channels(i, fn)
        print("Thread's #", fn[21], x,  'Finished', li.count(1), "/", len(li), sep='.')


def main():
    start = time()
    number_threads = 5
    print(datetime.now().time())
    c = mysql.connector.connect(user='root', database='m', password=password, charset="utf8",  use_unicode=True)
    cursor = c.cursor()
    try:
        for i in range(len(identity['api_id'])):
            li = []
            fn = identity['file'][i]
            filename = '/home/user/T/problems' + str(i) + '.txt'    # problems file
            with open(fn, "r") as f:
                for line in f:
                    li.append(line[:-1])
            client = establish_connection(identity['api_id'][i], identity['api_hash'][i], identity['session'][i])
            threads = []
            lock = threading.Lock()
            for chunk_list in (list(chunks(li, 100))):
                li_x = list(chunks_threads(chunk_list, number_threads))
                for x in range(number_threads):
                    thread = threading.Thread(target=func, args=(client, li_x[x], x, c, cursor, lock, filename))
                    threads.append(thread)
                    thread.start()
                for thread in threads:
                    thread.join()
                print('++++++++++++++++++sssssss++++++++++++++++++')
        print('=======SSS========================')
    finally:
        cursor.close()
        c.close()
        print('Done in: ', time() - start)


if __name__ == '__main__':
        main()
