import sys
import requests
import python_socks

import modules.common_mod.common as common


class Proxy(common.CommonFunc):
	IP_ERROR_MSG = '<error>'

	def __init__(self, *args, **kwargs):
		super().__init__(**kwargs)

		self.ip = ''
		self.port = ''
		self.port_socks5 = ''
		self.user = ''
		self.psw = ''
		self.ssl = True
		self.check_ip_result = ''

		if self.debug_mode:
			#self.ip = '121.244.147.137'
			#self.port = '8080'
			pass

	def __str__(self):
		if self.ip == '' or self.ip.isspace():
			return None
		return f'Proxy ip={self.ip} port={self.port} port_socks5={self.port_socks5} user={self.user} psw={self.psw} ssl={str(self.ssl)} check_res={self.check_ip_result}'

	def get_ip_with_port(self):
		if self.user == '' or self.user.isspace():
			res = f'{self.ip}:{self.port}'
		else:
			res = f'{self.user}:{self.psw}@{self.ip}:{self.port}'
		return res

	def get_dict(self):
		if self.ip == '' or self.ip.isspace():
			return None
		ip_with_port = self.get_ip_with_port()
		http = 'https://' if self.ssl else 'http://'
		url = f'{http}{ip_with_port}'
		return  {"http": url, "https": url} 

	def get_dict_socks5(self):
		if self.ip == '' or self.ip.isspace():
			return None
		if self.user == '' or self.user.isspace():
			#return (socks.SOCKS5, self.ip, self.port_socks5)
			return {'proxy_type': 'socks5',
					'addr': self.ip,
					'port': self.port_socks5
					#'rdns': True  # (optional) whether to use remote or local resolve, default remote
					}
		else:
			#return (socks.SOCKS5, self.ip, int(self.port_socks5), self.user, self.psw)
			return {'proxy_type': 'socks5',
					'addr': self.ip,
					'port': self.port_socks5,
					'username': self.user,
					'password': self.psw
					#'rdns': True  # (optional) whether to use remote or local resolve, default remote
					}

	def check_ip(self, session = None, informing = True):
		service_www = 'icanhazip.com'
		http = 'https://' if self.ssl else 'http://'
		service_url = f'{http}{service_www}'

		if informing: self.msg(f'Checking ip starting ({service_url})...')
		
		if session is None:
			session = requests.Session()
			session.proxies = self.get_dict()

		if informing: self.msg('Proxy: '+str(session.proxies))

		try:
			result = session.get(service_url, timeout=5)
			ip_txt = result.text.strip()
			if informing: self.msg(f'ip = {ip_txt}')
		except Exception as e:
			ip_txt = self.IP_ERROR_MSG
			if informing: self.msg(str(e))
			if informing: self.msg(sys.exc_info())

		if informing: self.msg('Checking ip finished')

		self.check_ip_result = ip_txt

		return ip_txt

	def set_session_proxy(self, session, check = False):
		'''session == requests.Session'''
		proxy_dict = self.get_dict()
		if proxy_dict is None:
			return
		session.proxies = proxy_dict
		if check:
			self.check_ip(session)


class ProxyCassandra(Proxy):
	def __init__(self, *args, cass_db, id_project = None, id_proxy = None, **kwargs):
		super().__init__(**kwargs)

		self.cass_db = cass_db

		if not id_project is None:
			self.load_proxy_by_project(id_project)

	def load_proxy_by_project(self, id_project):
		res = self.cass_db.get_proxy_project(id_project)
		if len(res) == 0:
			self.msg(f'Proxy by project id = {id_project} not found')
			return
		params = res[0]

		self.ip   = str(params['ip']).strip()
		self.port = str(params['port']).strip()
		self.port_socks5 = str(params['port_socks5']).strip()
		self.user = str(params['user']).strip()
		self.psw  = str(params['psw']).strip()
		self.ssl  = str(params['ssl']).strip()

if __name__ == "__main__":
	tst = Proxy(debug_mode = True, msg_func = print)
	#tst.ssl = False
	tst.check_ip()

	session = requests.Session()
	tst.check_ip(session)
