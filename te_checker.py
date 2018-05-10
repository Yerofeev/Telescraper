from telethon import TelegramClient
from time import sleep, time
import os
import sys
from telethon import errors
import random
import datetime
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
        print(str(e) + channel_name)
        return -2
    except errors.FloodWaitError as e:
        print(e)
        return -1
    except errors.RPCError as e:
        print(e)
    except TypeError as e:
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
    with open(identity['file'][index], 'r+') as f:
        for i, channel_name in enumerate(f):
            #sleep(random.uniform(1, 2))
            channel = get_channel(client, channel_name)
            if not channel:
                #  not used names or non-existing channels
                delete_row(channel_name, identity['file'][index])
                with open(not_used_names, 'a') as f_bad:
                    f_bad.write(channel_name)
            elif channel == -2:
                #not occupied
                print('Not occupied', end='')
                print(i, channel_name)
                delete_row(channel_name, identity['file'][index])
                with open(not_used_names, 'a') as f_not_used:
                    f_not_used.write(channel_name)
            elif channel == -1:
                # non-resolved channels
                print("Flood wait for", end='')
                print(i, channel_name)
                # delete this channel from origins
                delete_row(channel_name, identity['file'][index])
                # write this channel_name to corresponding file
                with open(telegram_txt, 'a') as f_bad:
                    f_bad.write(channel_name)
            elif channel:
                # good channels
                print("OK for ",end='')
                print(index, ' ', i, channel_name)

def main():
    for i in range(len(identity['session'])):
        session = identity['session'][i]
        api_id = identity['api_id'][i]
        api_hash = identity['api_hash'][i]
        client = establish_connection(session, api_id, api_hash, i)
        if client:
            main_loop(client, i)


if __name__ == '__main__':
        main()
