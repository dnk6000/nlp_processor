import modules.db.pg as pg

#core of wrappers
def do_with_query_plan(*args, func, autocommit = True, **kwargs):
    plan_id = 'plan_'+func.__qualname__
    self = args[0]

    if not self._is_plan_exist(plan_id):
        select = func(*args, **kwargs)                          #x
        pg.gvars.GD[plan_id] = self._prepare(select[0], select[1]) #y

    params = list(args[1:])
    for kwarg in kwargs:
        params.append(kwargs[kwarg])
        
    try:
        res = self._execute(plan_id, params)      #z
    except Exception as e:
        if self._is_it_InvalidSqlStatementName(e):
            select = func(*args, **kwargs)                          #x
            pg.gvars.GD[plan_id] = self._prepare(select[0], select[1]) #y
            res = self._execute(plan_id, params)  #z
        else:
            raise

    return res

#wrapper 
def execute_with_query_plan(func):
    def execute_with_plan(*args, autocommit = True, **kwargs):
        res = do_with_query_plan(*args, func = func, autocommit = autocommit, **kwargs)
        self = args[0]
        self.commit(autocommit)
        return self._convert_select_result(res)

    return execute_with_plan

#wrapper 
def execute_with_query_plan_0(func):
    def execute_with_plan(*args, autocommit = True, **kwargs):
        res = do_with_query_plan(*args, func = func, **kwargs)
        self = args[0]
        self.commit(autocommit)
        res = self._convert_select_result(res)
        return None if res is None else res[0]

    return execute_with_plan

#wrapper 
def select_with_query_plan(func):
    def execute_with_plan(*args, **kwargs):
        res = do_with_query_plan(*args, func = func, **kwargs)
        self = args[0]
        return self._convert_select_result(res)

    return execute_with_plan

#wrapper 
def select_with_query_plan_0(func):
    def execute_with_plan(*args, **kwargs):
        res = do_with_query_plan(*args, func = func, **kwargs)
        self = args[0]
        res = self._convert_select_result(res)
        return None if res is None else res[0]

    return execute_with_plan

