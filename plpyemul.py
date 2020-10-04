import psycopg2

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

                return self.cursor.execute('execute %s (%s)' % (args[0], _params_str) , **kwargs)

        return self.cursor.execute(*args, **kwargs)

    def commit(self, *args, **kwargs):
        return self.connection.commit(*args, **kwargs)

    def prepare(self, pgstatement, params):
        ''' example params: ["text", "text", "integer", "text", "text", "boolean", "integer"]
        '''
        plan_name = 'py_plan_' + str(PlPy.__get_next_plan_num__())

        _pgstatement = 'prepare ' + plan_name + ' as ' + pgstatement

        self.cursor.execute(_pgstatement, params)

        return plan_name

