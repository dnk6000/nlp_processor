
class CommonFunc:
	def __init__(self, debug_mode = True, msg_func = None, **kwargs):
		self.msg_func	 = msg_func
		self.debug_mode   = debug_mode

	def msg(self, message):
		if not self.msg_func == None:
			try:
				self.msg_func(str(message))
			except:
				pass

	def debug_msg(self, message):
		if not self.debug_mode:
			return
		self.msg(message)

