import sys
import requests
import python_socks

from requests.adapters import HTTPAdapter #for managing TLS version
import ssl                                #for managing TLS version

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
			self.ip = '5.180.103.152'
			self.port = '45785'
			self.user = 'Selfedot7'
			self.psw = 'F4o5LlN'
			pass

	def __repr__(self):
		if self.ip == '' or self.ip.isspace():
			return 'Empty ip. None proxy.'
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
		res = self.cass_db.git000_cfg.get_proxy_project(id_project)
		if res is None or len(res) == 0:
			self.msg(f'Proxy by project id = {id_project} not found')
			return
		params = res

		self.ip   = str(params['ip']).strip()
		self.port = str(params['port']).strip()
		self.port_socks5 = str(params['port_socks5']).strip()
		self.user = str(params['user']).strip()
		self.psw  = str(params['psw']).strip()
		self.ssl  = str(params['ssl']).strip()

class HTTPAdapterForProxy(HTTPAdapter):
	''' for managing TLS version '''

	def init_poolmanager(self, *args, **kwargs):
		"""Called to initialize the HTTPAdapter when a proxy is NOT used."""
		ssl_context = ssl.create_default_context()

		#ssl_context.options &= ~ssl.OP_NO_TLSv1_3 & ~ssl.OP_NO_TLSv1_2 & ~ssl.OP_NO_TLSv1_1
		#ssl_context.options &= ~ssl.OP_NO_TLSv1_3 & ~ssl.OP_NO_TLSv1_2
		ssl_context.minimum_version = ssl.TLSVersion.TLSv1_1
		ssl_context.maximum_version = ssl.TLSVersion.TLSv1_1

		# Also you could try to set ciphers manually as it was in my case.
		# On other ciphers their server was reset the connection with:
		# [Errno 104] Connection reset by peer
		# ssl_context.set_ciphers("ECDHE-RSA-AES256-SHA")

		# See urllib3.poolmanager.SSL_KEYWORDS for all available keys.
		kwargs["ssl_context"] = ssl_context

		return super().init_poolmanager(*args, **kwargs)

	def proxy_manager_for(self, proxy, **kwargs):
		"""Called to initialize the HTTPAdapter when a proxy is used."""
		
		kwargs['ssl_version'] = ssl.PROTOCOL_TLSv1_1
		#kwargs['ssl_version'] = ssl.PROTOCOL_TLSv1_2
		#kwargs['ssl_version'] = ssl.PROTOCOL_TLS_CLIENT
		#kwargs['ssl_version'] = ssl.PROTOCOL_TLS_SERVER
		#kwargs['ssl_version'] = ssl.PROTOCOL_TLS
		#kwargs['ssl_version'] = ssl.PROTOCOL_TLSv1
		#kwargs['ssl_version'] = ssl.PROTOCOL_SSLv23

		return super().proxy_manager_for(proxy, **kwargs)


def _debug_tls():
	adapter = HTTPAdapterForProxy()

	tst = Proxy(debug_mode = True, msg_func = print)
	#tst.ssl = False
	sess = requests.Session()
	
	sess.mount('http://', adapter)
	sess.mount('https://', adapter)
	
	tst.check_ip(session = sess)

	res = sess.get('https://www.howsmyssl.com/a/check', verify=True)
	js = res.json()
	tls_actual = js['tls_version']
	print(f'tls_actual without proxy {tls_actual}')

	#----------
	sess.proxies = tst.get_dict()

	tst.check_ip(session = sess)
	
	res = sess.get('https://www.howsmyssl.com/a/check', verify=True)
	js = res.json()
	tls_actual = js['tls_version']
	print(f'tls_actual proxy {tls_actual}')

	#session = requests.Session()
	#tst.check_ip(session)
	#sess.rebuild_proxies()

def _debug_proxy():
	tst = Proxy(debug_mode = True, msg_func = print)
	tst.check_ip()

if __name__ == "__main__":
	#_debug_tls()

	#_debug_proxy()

	pass
