import modules.common_mod.const as const
import modules.common_mod.common as common
import modules.common_mod.exceptions as exceptions
import modules.common_mod.date as date

from modules.common_mod.globvars import GlobVars
if const.PY_ENVIRONMENT: 
    GD = None
gvars = None

#common wrapper 
def execute_with_query_plan(func):
    def execute_with_plan(*args, **kwargs):
        plan_id = 'plan_'+func.__qualname__
        self = args[0]

        if not self._is_plan_exist(plan_id):
            select = func(*args, **kwargs)
            gvars.GD[plan_id] = self._prepare(select[0], select[1])

        params = list(args[1:])
        for kwarg in kwargs:
            params.append[kwargs[kwarg]] 
        res = self._execute(plan_id, params)

        return self._convert_select_result(res)

    return execute_with_plan

class PgDb(common.CommonFunc):
    plpy = None
    memo_pgconn_ptr = -1
    db_error_counter = {}
    db_error_limit = 10  #number errors in a row (=подряд)

    def __init__(self, plpy = None, GD = None, **kwargs):
        common.CommonFunc.__init__(self, **kwargs)

        if PgDb.plpy is None:
            PgDb.plpy = plpy

        global gvars
        if gvars is None:
            gvars = GlobVars(GD)  #must calls only once

    def _clear_query_plans(self):
        keys_to_remove = []
        for key in gvars.GD:
            if key[0:5] == 'plan_':
                keys_to_remove.append(key)
        for key in keys_to_remove:
            gvars.GD.pop(key)
        pass

    def _is_plan_exist(self, plan_id):
        result = True
        if const.PY_ENVIRONMENT:
            if PgDb.plpy.connection is None:
                result = False
            elif PgDb.memo_pgconn_ptr != PgDb.plpy.connection.pgconn_ptr:
                PgDb.memo_pgconn_ptr = PgDb.plpy.connection.pgconn_ptr
                self._clear_query_plans()
                result = False
        if result and (not plan_id in gvars.GD or gvars.GD[plan_id] is None):
            result = False
        return result

    def _check_db_error_limit(self, plan_id, _exception):
        if _exception is None:
            PgDb.db_error_counter[plan_id] = 0
        else:
            if plan_id in PgDb.db_error_counter:
                PgDb.db_error_counter[plan_id] += 1
            else:
                PgDb.db_error_counter[plan_id] = 1
            if PgDb.db_error_counter[plan_id] >= PgDb.db_error_limit:
                PgDb.plpy.notice(f'Limit of write errors in the database reached! ({self.__module__}, {str(self.__class__)})')
                raise _exception

    def _prepare(self, pgstatement, params):
        return PgDb.plpy.prepare(pgstatement, params)

    def _execute(self, plan_id, var_list, exept = None):
        try:
            res = PgDb.plpy.execute(gvars.GD[plan_id], var_list)
            self._check_db_error_limit(plan_id, None)
            return res
        except Exception as e:
            exept = {'exeption': e, 'description': exceptions.get_err_description(e, plan_id = plan_id, var_list = var_list)}
            self._check_db_error_limit(plan_id, e)
            return None

    def commit(self, autocommit = True):
        if autocommit:
            PgDb.plpy.commit()

    def rollback(self):
        PgDb.plpy.rollback()

    def custom_simple_request(self, request_txt, autocommit = True, **kwargs):
        plan_id = 'plan_custom_simple_request'
        gvars.GD[plan_id] = PgDb.plpy.prepare(request_txt, [])
        res = self._execute(plan_id,[])
        self.commit(autocommit)
        return None if res is None else self._convert_select_result(res)

    def _convert_select_result(self, res):
        return res


class PgDbCassandra(PgDb):

    def __init__(self, plpy = None, GD = None):
        super().__init__(plpy, GD)

    def _execute(self, plan_id, var_list, id_project = 0):
        exept = None
        res = super()._execute(plan_id, var_list, exept)
        if exept is not None:
            self.log_error(const.CW_RESULT_TYPE_DB_ERROR, id_project, exept['description'])
        return res

    def _convert_select_result(self, res, str_to_date_conv_fields = [], decimal_to_float_conv_fields = []):

        #plpy in PG return date fields in str format, therefore, a conversion is required
        if const.PG_ENVIRONMENT and len(str_to_date_conv_fields) > 0:

            converter = date.StrToDate('%Y-%m-%d %H:%M:%S+.*')

            for field in str_to_date_conv_fields:
                for row in res:
                    row[field] = converter.get_date(row[field], type_res = 'D')
                
        if len(decimal_to_float_conv_fields) > 0:
            for field in decimal_to_float_conv_fields:
                for row in res:
                    row[field] = row[field].__float__()  #class Decimal to float

        #return [row[0] for row in res]
        return res
        

class Cassandra(PgDbCassandra):
    def __init__(self, plpy, GD, **kwargs):
        super().__init__(plpy, GD)
        self.git010_dict = Cassandra_git010_dict(**kwargs)
        self.git430_ner  = Cassandra_git430_ner(**kwargs)

        if gvars is not None and not gvars.initialized:
            self._initialize_gvars()
        

    def _initialize_gvars(self):
        if not gvars.initialized:
            gvars.set('VK_SOURCE_ID', self.git010_dict.get_www_source_id('vk'))
            gvars.set('TG_SOURCE_ID', self.git010_dict.get_www_source_id('tg'))

            ner_ent_types = {}
            for i in self.git430_ner.ent_type_select_all():
                ner_ent_types[i['name']] = { 'id': i['id'], 'not_entity': i['not_entity'] }
            gvars.set('NER_ENT_TYPES', ner_ent_types)

            gvars.initialize()


class Cassandra_git010_dict(PgDbCassandra):
    ''' Functions of db scheme git010_dict '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    @execute_with_query_plan
    def _get_www_source_id(self, www_source_name):
        return (''' SELECT git010_dict.get_www_sources_id($1) ''', 
                ["text"])
    
    def get_www_source_id(self, www_source_name):
        res = self._get_www_source_id(www_source_name)
        return res[0]['get_www_sources_id']


class Cassandra_git430_ner(PgDbCassandra):
    ''' Functions of db scheme git010_dict '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    @execute_with_query_plan
    def ent_type_select_all(self):
        return ('select * from git430_ner.ent_type_select_all();', 
                [])

    def __ent_type_select_all(self):
        plan_id = 'plan_ent_type_select_all'
        if not self._is_plan_exist(plan_id):
            pg_func = 'select * from git430_ner.ent_type_select_all();'
            gvars.GD[plan_id] = self._prepare(pg_func, [])

        #res = self._execute(gvars.GD[plan_id], [])
        res = self._execute(plan_id, [])

        return self._convert_select_result(res)


if __name__ == "__main__":
    if const.PY_ENVIRONMENT:
        import ModulesPyOnly.plpyemul as plpyemul
        plpy = plpyemul.get_plpy()    

    cass_db = Cassandra(plpy, GD)

    cass_db._is_plan_exist('111')

    f=1

