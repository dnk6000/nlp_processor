import logging

import Modules.Common.const as const

class CommonFunc:
	def __init__(self, *args, debug_mode = True, msg_func = None, **kwargs):
		self.msg_func	 = msg_func
		self.debug_mode  = debug_mode

	def msg(self, message):
		if not self.msg_func is None:
			try:
				self.msg_func(str(message))
			except:
				pass

	def debug_msg(self, message):
		if not self.debug_mode:
			return
		self.msg(message)

class Logger:
	def __init__(self, log_name, *args, **kwargs):

		self.log_name = log_name
		self.log_file_name = const.LOG_FOLDER + log_name + ".log"

		self.logger = logging.getLogger(log_name)
		self.logger.setLevel(logging.INFO)
	
		fh = logging.FileHandler(self.log_file_name, 'w', 'utf-8')

		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		fh.setFormatter(formatter)
	
		self.logger.addHandler(fh)

	def info(self, message):
		self.logger.info(str(message))

def clear_file(fname):
    f = open(fname, 'w')
    f.close()    
