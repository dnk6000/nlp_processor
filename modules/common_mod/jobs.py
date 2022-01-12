import configparser
import time

import modules.common_mod.const as const
import modules.common_mod.common as common

class JobManager(common.CommonFunc):
	def __init__(self, *args, id_job, db, **kwargs):
		super().__init__(*args, **kwargs)

		self.id_job = id_job
		self.db = db
		self.job_params = {}
		self.job_steps = {}
		self.current_step = ''
		self.no_job = True

		if id_job is None:
			self.once = True
		else:
			self.once = False
			self._read_from_db()
			self._parse_job_program()

	def __repr__(self):
		if self.no_job:
			return f'No job. id = {self.id_job}'
		return f'id = {self.id_job} {self.job_params["name"]}'

	def _get_step_default_params(self):
		return {
			'id_project': 0,
			'step_name': 'None',
			'num_subscribers_1': 0,
			'num_subscribers_2': 0,
			'next_step': '',
			'pause_sec_between_steps': 60,
			'max_errors': 3,
			'_runing': False
			}

	def _read_from_db(self):
		self.debug_msg(f'Read job id {self.id_job}');
		res = self.db.job_read(self.id_job)
		if len(res) > 0:
			self.job_params = res[0]
			self.no_job = False
		else:
			self.no_job = True

	def _parse_job_program(self):
		cfg = configparser.ConfigParser()
		cfg.read_string(self.job_params['program'])

		self.job_steps = {}
		first_step = ''

		for step in cfg.sections():
			if first_step == '':
				first_step = step
			step_keys = self._get_step_default_params()
			for key in cfg[step]:  
				if key in step_keys:
					if type(step_keys[key]) == int:
						step_keys[key] = int(cfg[step][key])
					elif type(step_keys[key]) == str:
						step_keys[key] = str(cfg[step][key])

			self.job_steps[step] = step_keys

		if self.current_step == '' or not self.current_step in self.job_steps:
			self.current_step = first_step

	def get_step_params(self):
		if self.once:
			return self._get_step_default_params()
		else:
			return self.job_steps[self.current_step]

	def _turn_off(self):
		self.debug_msg(f'Read job id {self.id_job}');
		self.job_params = self.db.job_turn_off(self.id_job)

	def _sleep(self):
		time.sleep(30)
		pass

	def get_next_step(self):
		if self.once:
			self.once = False
			return True

		if self.no_job:
			self._log_finish()
			return False

		prev_program = self.job_params['program']
		self._read_from_db()
		
		if self.no_job:
			self._log_finish()
			return False
		
		if not self.job_params['enabled']:
			self._log_finish()
			return False
		
		if prev_program != self.job_params['program']:
			self._parse_job_program()
			self._log_program_reload()

		if self.current_step == '':
			self._log_finish()
			return False

		step = self.job_steps[self.current_step]
		if not step['_runing']:
			step['_runing'] = True
			return True
		step['_runing'] = False
		if step['next_step'] == '' or not step['next_step'] in self.job_steps:
			self.current_step = ''
			self._log_finish()
			return False
		self.current_step = step['next_step']
		self.job_steps[self.current_step]['_runing'] = True
		time.sleep(step['pause_sec_between_steps'])
		self._log_step_start()
		return True

	def _log_start(self):
		self.db.log_info(const.LOG_INFO_JOB_START, 0, self.__repr__())
		pass

	def _log_step_start(self):
		self.db.log_info(const.LOG_INFO_JOB_STEP_START, 0, f'{self.__repr__()} Step: {self.current_step}')
		pass

	def _log_program_reload(self):
		self.db.log_info(const.LOG_INFO_JOB_RELOAD_PROGRAM, 0, self.__repr__())
		pass

	def _log_finish(self):
		self.db.log_info(const.LOG_INFO_JOB_FINISH, 0, self.__repr__())
		pass