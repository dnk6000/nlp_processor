import psycopg2
from psycopg2.extras import RealDictCursor
import re
import time

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

        self._number_of_tries = 3
        self._tries_pause = 6 #sec

    def _connect(self):
        if self.connection == None:
            self.connection = psycopg2.connect(**self.connection_par)
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

    @classmethod
    def __get_next_plan_num__(cls):
        cls.NUM_PREP_PLAN += 1
        return cls.NUM_PREP_PLAN

    def subtransaction(self):
        #https://stackoverflow.com/questions/29773002/creating-transactions-with-with-statements-in-psycopg2
        self._connect()
        return self.connection

    @staticmethod
    def notice(message):
        print(message)

    def _execute(self, *args, **kwargs):
        self._connect()
        
        result = None

        successfully = False
        attempt = 0

        while not successfully and attempt < self._number_of_tries:
            try:
                result = self.cursor.execute(*args, **kwargs)
                successfully = True
            except Exception as expt:
                attempt += 1

                if attempt >= self._number_of_tries:
                    raise expt
                else:
                    print('Ошибка записи в БД !!! Попытка '+str(attempt))
                    time.sleep(self._tries_pause)

        return result

    def cursor(self, *args, **kwargs):
        #https://postgrespro.ru/docs/postgrespro/9.5/plpython-database
        pass

    def execute(self, *args, **kwargs):
        self._connect()
        
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

        if self._return_select_result[_plan_name]:
            self.cursor.execute(*args, **kwargs)
            result = self.cursor.fetchall()
            return self._convert_select_result(result)
        else:
            return self.cursor.execute(*args, **kwargs)

    def _convert_select_result(self, res):
        #!!now returning the list of tuple, but plpy returning the list of dict !!
        #!!need refactoring later
        return res

    def commit(self, *args, **kwargs):
        self._connect()
        return self.connection.commit(*args, **kwargs)

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

        return plan_name

    def _define_return_result(self, pgstatement, plan_name):
        self._return_select_result[plan_name] = False

        re_vars = re.search(r'RETURNING', pgstatement, re.IGNORECASE)
        if re_vars != None:
            self._return_var_names[plan_name] = True
        else:
            self._return_var_names[plan_name] = False

            re_select = re.search(r'^(\n|\s)*SELECT', pgstatement, re.IGNORECASE)
            if re_select != None:
                self._return_select_result[plan_name] = True

    def __del__(self):
        #print('__del__')
        if self.connection != None:
            self.connection.close()