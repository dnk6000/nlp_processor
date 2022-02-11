import logging
from configparser import ConfigParser

import modules.common_mod.const as const

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

class ConfigParserNoSection(ConfigParser):
	def __init__(self, *args, debug_mode=True, msg_func=None, **kwargs):
		ConfigParser.__init__(self, *args, **kwargs)
		self.FAKE_SECTION = 'fake_section'

	def read_string(self, string):
		super(ConfigParser, self).read_string(f'[{self.FAKE_SECTION}]\n'+string)

	@property
	def keys(self):
		keys_dict = {}
		if self.FAKE_SECTION in self:
			for key in self[self.FAKE_SECTION]:
				if self[self.FAKE_SECTION][key] == 'True':
					keys_dict[key] = True
				elif self[self.FAKE_SECTION][key] == 'False':
					keys_dict[key] = False
				else:
					keys_dict[key] = self[self.FAKE_SECTION][key]
		return keys_dict

	@staticmethod
	def get_parameters_str(params:dict):
		res = ''
		for i in params:
			res = res + f'{str(i)} = {str(params[i])}\n'
		return res


def clear_file(fname):
    f = open(fname, 'w')
    f.close()    
