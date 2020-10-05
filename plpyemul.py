import psycopg2
import re

class PlPy(object):
    NUM_PREP_PLAN = 0

    def __init__(self,
                 database, 
                 host, 
                 port,
                 user, 
                 password):
        self.connection = psycopg2.connect(
          database = database, 
          user = user, 
          password = password, 
          host = host, 
          port = port
        )
        self.cursor = self.connection.cursor()
        self._return_var_names = {}

    @classmethod
    def __get_next_plan_num__(self):
        PlPy.NUM_PREP_PLAN += 1
        return PlPy.NUM_PREP_PLAN

    def notice(self, message):
        print(message)

    def execute(self, *args, **kwargs):
        if isinstance(args[0], str):
            if args[0][:8] == 'py_plan_':

                _params = []
                for i in args[1]:
                    if isinstance(i, str):
                        _params.append("'" + i + "'")
                    else:
                        _params.append(str(i))

                _params_str = ', '.join(_params)

                _plan_name = args[0]

                self.cursor.execute('execute %s (%s)' % (_plan_name, _params_str) , **kwargs)

                result = []

                if len(self._return_var_names[_plan_name]) != 0:
                    #returning vars present
                    try:
                        res_list = self.cursor.fetchone()
                        result = [dict( zip(self._return_var_names[_plan_name], res_list) )]
                    except Exception as expt:
                        if str(expt) == 'no results to fetch':
                            result = [{}]
                        else:
                            raise expt

                return result

        return self.cursor.execute(*args, **kwargs)

    def commit(self, *args, **kwargs):
        return self.connection.commit(*args, **kwargs)

    def prepare(self, pgstatement, params):
        ''' example params: ["text", "text", "integer", "text", "text", "boolean", "integer"]
            sql syntax for returning vars must be:  "RETURNING xxx AS yyy" (case ignore)
        '''
        plan_name = 'py_plan_' + str(PlPy.__get_next_plan_num__())

        _pgstatement = 'prepare ' + plan_name + ' as ' + pgstatement

        self.cursor.execute(_pgstatement, params)

        self._define_return_var_names(pgstatement, plan_name)

        return plan_name

    def _define_return_var_names(self, pgstatement, plan_name):
        '''sql syntax for returning vars must be:  "RETURNING xxx AS yyy" (case ignore)
        '''
        re_vars = re.search(r'RETURNING (.+)', pgstatement, re.IGNORECASE)
        if re_vars != None:
            self._return_var_names[plan_name] = re.findall(r'\w+ as (\w+)', re_vars.groups()[0])
        else:
            self._return_var_names[plan_name] = []

