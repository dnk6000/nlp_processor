import modules.db.pg as pg

#core of wrappers
def do_with_query_plan(*args, func, 
                              autocommit = True, 
                              str_to_date_conv_fields = [],      #param used here only to exclude from kwargs 
                              decimal_to_float_conv_fields = [], #param used here only to exclude from kwargs
                              **kwargs):
    plan_id = 'plan_'+func.__qualname__
    self = args[0]

    if not self._is_plan_exist(plan_id):
        query = func(*args, **kwargs)                            #x
        pg.gvars.GD[plan_id] = self._prepare(query[0], query[1]) #y

    params = list(args[1:])
    for kwarg in kwargs:
        params.append(kwargs[kwarg])
    
    try:
        res = self._execute(plan_id, params)                     #z
    except Exception as e:
        if self._is_it_InvalidSqlStatementName(e):
            if autocommit:
                self.rollback()
                self.subtransaction()
            query = func(*args, **kwargs)                             #x
            pg.gvars.GD[plan_id] = self._prepare(query[0], query[1])  #y
            res = self._execute(plan_id, params)                      #z
        else:
            raise

    self.commit(autocommit)

    return res

#wrapper 
def execute_with_query_plan(func):
    def execute_with_plan(*args, **kwargs):
        if not 'str_to_date_conv_fields' in kwargs:
            kwargs['str_to_date_conv_fields'] = []
        if not 'decimal_to_float_conv_fields' in kwargs:
            kwargs['decimal_to_float_conv_fields'] = []
        if not 'autocommit' in kwargs:
            kwargs['autocommit'] = True

        res = do_with_query_plan(*args, func = func, **kwargs)
        self = args[0]
        return self._convert_select_result(res, 
                                           str_to_date_conv_fields = kwargs['str_to_date_conv_fields'],
                                           decimal_to_float_conv_fields = kwargs['decimal_to_float_conv_fields'])

    return execute_with_plan

#wrapper 
def execute_with_query_plan_0(func):
    def execute_with_plan(*args, **kwargs):
        if not 'str_to_date_conv_fields' in kwargs:
            kwargs['str_to_date_conv_fields'] = []
        if not 'decimal_to_float_conv_fields' in kwargs:
            kwargs['decimal_to_float_conv_fields'] = []
        if not 'autocommit' in kwargs:
            kwargs['autocommit'] = True

        res = do_with_query_plan(*args, func = func, **kwargs)
        self = args[0]
        res = self._convert_select_result(res, 
                                          str_to_date_conv_fields = kwargs['str_to_date_conv_fields'],
                                          decimal_to_float_conv_fields = kwargs['decimal_to_float_conv_fields'])
        return None if res is None or len(res) == 0 else res[0]

    return execute_with_plan

#wrapper 
def select_with_query_plan(func):
    def execute_with_plan(*args, **kwargs):
        if not 'str_to_date_conv_fields' in kwargs:
            kwargs['str_to_date_conv_fields'] = []
        if not 'decimal_to_float_conv_fields' in kwargs:
            kwargs['decimal_to_float_conv_fields'] = []
        if not 'autocommit' in kwargs:
            kwargs['autocommit'] = False

        res = do_with_query_plan(*args, func = func, **kwargs)
        self = args[0]
        return self._convert_select_result(res, 
                                           str_to_date_conv_fields = kwargs['str_to_date_conv_fields'],
                                           decimal_to_float_conv_fields = kwargs['decimal_to_float_conv_fields'])

    return execute_with_plan

#wrapper 
def select_with_query_plan_0(func):
    def execute_with_plan(*args, **kwargs):
        if not 'str_to_date_conv_fields' in kwargs:
            kwargs['str_to_date_conv_fields'] = []
        if not 'decimal_to_float_conv_fields' in kwargs:
            kwargs['decimal_to_float_conv_fields'] = []
        if not 'autocommit' in kwargs:
            kwargs['autocommit'] = False

        res = do_with_query_plan(*args, func = func, **kwargs)
        self = args[0]
        res = self._convert_select_result(res, 
                                          str_to_date_conv_fields = kwargs['str_to_date_conv_fields'],
                                          decimal_to_float_conv_fields = kwargs['decimal_to_float_conv_fields'])
        return None if res is None or len(res) == 0 else res[0]

    return execute_with_plan

