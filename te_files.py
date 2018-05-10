import time
import random
from functools import partial
import subprocess
import os
from credentials import identity, telegram_txt, fn_test0, fn_test1, good_s1, good_s2, origin


class tracer:
    def __init__(self, func): # On @ decoration: save original func
        self.calls = 0
        self.func = func
    def __call__(self, *args): # On later calls: run original func
        self.calls += 1
        print('call %s to %s' % (self.calls, self.func.__name__))
        self.func(*args)

@tracer
def delete_row(channel_name, telegram_txt):
    """ Delete specific row(channel_name) in  file consisting of channel_names"""
    delete_row
    f = open(telegram_txt, "r+")
    d = f.readlines()
    f.seek(0)
    for i in d:
        if i != channel_name:
            f.write(i)
    f.truncate()
    f.close()


def dup_1_file(k):
	"""to delete duplicate lines from files"""
	for i in range(k):
		lines_seen = set() # holds lines already seen
		fn = '/home/user/Work/Telescraper/T/good' + str(i+1) + '_.txt'
		outfile = open(fn, "w")
		print(fn)
		for line in open(identity['file'][i], "r"):
			if line not in lines_seen:     # not a duplicate
				outfile.write(line)
				lines_seen.add(line)
			else:
				print('dupl line: ', line)
		outfile.close()


def delete_duplicate_lines(j):
	for i in range(k):
		for j in range(1, k - i):
			f = open(identity['file'][i], 'r')
			unique_lines = f.readlines()
			print(i, j+i)
			with open(identity['file'][j+i], 'r') as f:
				for channel_name in f:
					if channel_name in unique_lines:
						print(channel_name)
						delete_row(channel_name, identity['file'][j+i])
			f.close()


def count_lines(k):
	"""counts lines in files"""
	count_all = 0
	for i in range(k):
		with open(identity['file'][i], 'r') as f:
			for p, q in enumerate(f):
				pass
			count_all += p+1
	print(count_all)


def mock_files(k):
	"""Creates 10-lines channel files to test"""
	for i in range(k):
		count = 0
		print(i)
		with open(identity['mock_file'][i], 'a+') as output:
			with open(identity['file'][i], 'r') as input:
				for line in input:
					print(line)
					output.write(line)
					count += 1
					if count > 10:
						break

def all_files(k):
	"""create one file from all good files to compare with original"""
	with open('/home/user/Work/Telescraper/T/good.txt', 'a+') as good:
		for i in range(k):
			for line in open(identity['file'][i], "r"):
				good.write(line)
		for line in open('/home/user/Work/Telescraper/T/not_used_names.txt', 'r'):
			good.write(line)

def file_diff():
	f1 = open('/home/user/Work/Telescraper/T/good.txt', "r")
	g1 = set(f1.readlines())
	f2 = open('/home/user/Work/Telescraper/T/original.txt', "r")
	g2 = set(f2.readlines())
	g = g2 - g1
	with open(telegram_txt, 'a+') as t:
		for line in g:
			t.write(line)
	f1.close()
	f2.close()
	

if __name__ == '__main__':
	k = len(identity['api_id'])
	while True:
		opt = input("Enter 'q' or func.__name__ name: ")
		if opt == 'q' or opt == 'Q':
			break
		elif opt == 'delete_duplicate_lines':
			delete_duplicate_lines(k)
		elif opt == 'count_lines':
			count_lines(k)
		elif opt == 'mock_files':
			mock_files(k)
		elif opt == 'dup_1_file':
			dup_1_file(k)
		elif opt == 'all_files':
			all_files(k)
		elif opt == 'file_diff':
			file_diff()

