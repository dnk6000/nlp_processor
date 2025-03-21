import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.errors import InvalidSqlStatementName as InvalidSqlStatementName_psycopg
from psycopg2.errors import InFailedSqlTransaction as InFailedSqlTransaction_psycopg

import re
import time

import modules.common_mod.const as const
import modules.common_mod.pauser as pauser
import modules.common_mod.date as date

import ModulesPyOnly.self_psw as self_psw

#Хранимые процедуры на Python в PostgreSQL https://tproger.ru/articles/stored-procedures-on-python-in-postgresql/
#Миллион строк в секунду из Postgres с помощью Python https://habr.com/ru/post/317394/

class PlPy(object):
    NUM_PREP_PLAN = 0

    def __init__(self, database = '', host = '', port = '', user = '', password = ''):
        self.connection_par = { 'database' : database, 
                                'host'     : host, 
                                'port'     : port, 
                                'user'     : user, 
                                'password' : password
                              }
        
        self.connection = None        
        self.cursor = None        
        
        self._return_var_names = {}
        self._return_select_result = {}

        self._number_of_tries = 5

        self._pauser = pauser.ExpPauser(delay_seconds = 3.9, number_intervals = self._number_of_tries) #~15 мин

        #self._cached_plans = {}
        pass

    def __del__(self):
        #print('__del__')
        if self.connection is not None:
            self.connection.close()


    @classmethod
    def __get_next_plan_num__(cls):
        cls.NUM_PREP_PLAN += 1
        return cls.NUM_PREP_PLAN


    def _connect(self):
        if self.connection is None:
            successfully = False
            attempt = 0

            while not successfully and attempt <= self._number_of_tries:
                try:
                    self.connection = psycopg2.connect(**self.connection_par)
                    self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
                    self.cursor.tzinfo_factory = None #from psycopg2.tz import LocalTimezone
                    successfully = True
                except Exception as expt:
                    time.sleep(2)
                    attempt += 1

                    if attempt >= self._number_of_tries:
                        raise expt
                    else:
                        print(f'{date.date_now_str()}: Ошибка соединения с БД !!! Попытка {str(attempt)}')

    def _execute(self, *args, **kwargs):
        self._connect()
        
        result = None

        successfully = False
        attempt = 0

        while not successfully and attempt <= self._number_of_tries:
            try:
                result = self.cursor.execute(*args, **kwargs)
                successfully = True
            
            except InvalidSqlStatementName_psycopg as expt:
                try:
                    self.rollback()
                except:
                    pass
                match = re.search(r'prepared statement \"(\w*)\" does not exist', expt.args[0]) 
                if match:
                    plan = match.groups()[0]
                else:
                    plan = ''
                raise InvalidSqlStatementName(plan)

            except InFailedSqlTransaction_psycopg as expt:
                self.rollback()
                raise InFailedSqlTransaction()

            except Exception as expt:
                attempt += 1
                self.notice(str(expt))

                if not self._pauser.sleep():
                    raise expt
                else:
                    if type(expt) == psycopg2.OperationalError and len(expt.args) >= 1 and isinstance(expt.args[0], str) \
                        and ('server closed the connection' in expt.args[0] \
                             or 'could not receive data from server' in expt.args[0]):
                        self.notice(f'{date.date_now_str()} Ошибка чтения/записи в БД !!! Попытка {str(attempt)}')
                        self.notice('  Trying to reconnect...')
                        self.connection = None
                        self._connect()
                    elif type(expt) == psycopg2.InterfaceError and len(expt.args) >= 1 and isinstance(expt.args[0], str) \
                        and ('cursor already closed' in expt.args[0]):
                        self.notice(f'{date.date_now_str()} Ошибка чтения/записи в БД !!! Попытка {str(attempt)}')
                        self.notice('  Trying to reconnect...')
                        self.connection = None
                        self._connect()
                    else:
                        self.notice(f'{date.date_now_str()} Ошибка чтения/записи в БД !!!')
                        #print('Error: ')
                        #print('     '+str(type(expt)))
                        #print('     '+str(expt.args))
                        #print('     '+str(expt))
                        raise expt
        
        if not successfully:
            raise expt

        return result

    def _convert_select_result(self, res):
        #!!now returning the list of tuple, but plpy returning the list of dict !!
        #!!need refactoring later
        return res

    def _define_return_result(self, pgstatement, plan_name):
        self._return_select_result[plan_name] = False

        re_vars = re.search(r'RETURNING', pgstatement, re.IGNORECASE)
        if re_vars is not None:
            self._return_var_names[plan_name] = True
        else:
            self._return_var_names[plan_name] = False

            re_select = re.search(r'^(\n|\s)*SELECT', pgstatement, re.IGNORECASE)
            if re_select is not None:
                self._return_select_result[plan_name] = True


    @staticmethod
    def notice(message):
        print(message)
        pass

    def subtransaction(self):
        #https://stackoverflow.com/questions/29773002/creating-transactions-with-with-statements-in-psycopg2
        self._connect()
        return self.connection

    def commit(self, *args, **kwargs):
        self._connect()
        return self.connection.commit(*args, **kwargs)

    def rollback(self, *args, **kwargs):
        self._connect()
        return self.connection.rollback(*args, **kwargs)

    def cursor(self, *args, **kwargs):
        #https://postgrespro.ru/docs/postgrespro/9.5/plpython-database
        pass

    def prepare(self, pgstatement, params):
        ''' example params: ["text", "text", "integer", "text", "text", "boolean", "integer"]
            sql syntax for returning vars must be:  "RETURNING xxx AS yyy" (case ignore)
        '''
        self._connect()

        plan_name = 'py_plan_' + str(PlPy.__get_next_plan_num__())

        _pgstatement = 'prepare ' + plan_name + ' as ' + pgstatement

        #self.cursor.execute(_pgstatement, params)
        self._execute(_pgstatement, params)

        self._define_return_result(pgstatement, plan_name)

        #self._cached_plans[plan_name] = (pgstatement, params)

        return plan_name

    def execute(self, *args, **kwargs):
        self._connect()
        
        _plan_name = ''
        
        if isinstance(args[0], str):
            if args[0][:8] == 'py_plan_':

                _plan_name = args[0]

                #_params = []
                #for i in args[1]:
                #    if isinstance(i, str):
                #        _params.append("'" + i + "'")
                #    else:
                #        _params.append(str(i))
                #_params_str = ', '.join(_params)
                #result = self._execute('execute %s (%s)' % (_plan_name, _params_str) , **kwargs)
                
                if args[1] is None or (isinstance(args[1],list) and len(args[1])==0): #without parameters
                    _params_str = ''
                    result = self._execute('execute '+_plan_name)
                else:
                    _params_str = ', '.join('%s' for i in range(len(args[1])))
                    result = self._execute('execute '+_plan_name+' ('+_params_str+')', tuple(args[1]))
               

                if self._return_var_names[_plan_name]:
                    #returning vars present
                    result = []
                    try:
                        result = self.cursor.fetchone()
                    except Exception as expt:
                        if str(expt) == 'no results to fetch':
                            result = [{}]
                        else:
                            raise expt
                elif self._return_select_result[_plan_name]:
                    result = self._convert_select_result(self.cursor.fetchall())

                return result

        if _plan_name != '' and self._return_select_result[_plan_name]:
            self.cursor.execute(*args, **kwargs)
            result = self.cursor.fetchall()
            return self._convert_select_result(result)
        else:
            return self.cursor.execute(*args, **kwargs)


def get_plpy():

    cassandra_db_conn_par = {
        'database': 'cassandra_new', 
        'host'   : '192.168.62.46', 
        'port': '5432', 
        'user': 'm.tyurin', 
        'password': self_psw.get_psw_db_mtyurin()
    }
    return PlPy(**cassandra_db_conn_par)

class spiexceptions(Exception):
    pass

class InvalidSqlStatementName(spiexceptions):
    def __init__(*args, plan = '', **kwargs):
        plan = plan

class InFailedSqlTransaction(spiexceptions):
    pass
