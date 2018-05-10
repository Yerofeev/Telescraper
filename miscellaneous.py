import sys
import datetime
# -----------------------Logging---------------------------------------- #


class Logger(object):
	def __init__(self, fn=''):
		self.terminal = sys.stdout
		self.log = open("/home/user/Work/Telescraper/logs/" + fn + datetime.datetime.now().strftime('_%H_%M_%d_%m_%Y.log'), "a+")

	def write(self, message):
		self.terminal.write(message)
		self.log.write(message)

	def flush(self):
		#this flush method is needed for python 3 compatibility.
		#this handles the flush command by doing nothing.
		#you might want to specify some extra behavior here.
		pass
# --------------------------------------------------------------------- #

