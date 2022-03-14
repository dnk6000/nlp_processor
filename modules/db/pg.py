import modules.common_mod.const as const
import modules.common_mod.common as common
import modules.common_mod.exceptions as exceptions

from modules.common_mod.globvars import GlobVars
gvars = None

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

    def _execute(self, plan_id, var_list, exept = {}):
        try:
            res = PgDb.plpy.execute(gvars.GD[plan_id], var_list)
            self._check_db_error_limit(plan_id, None)
            return res
        except Exception as e:
            exept = {'exeption': e, 'description': exceptions.get_err_description(e, plan_id = plan_id, var_list = var_list)}
            self.plpy.notice(str(exept))
            self._check_db_error_limit(plan_id, e)
            if self._is_it_InvalidSqlStatementName(e):
                raise
            #return None
            raise

    def _is_it_InvalidSqlStatementName(self, _exeption: Exception):
        return 'InvalidSqlStatementName' in str(type(_exeption))

    def subtransaction(self):
        return PgDb.plpy.subtransaction()

    def commit(self, autocommit = True):
        if autocommit:
            PgDb.plpy.commit()

    def rollback(self):
        PgDb.plpy.rollback()

    def free_query(self, request_txt, autocommit = True, **kwargs):
        plan_id = 'plan_custom_query'
        gvars.GD[plan_id] = PgDb.plpy.prepare(request_txt, [])
        res = self._execute(plan_id,[])
        self.commit(autocommit)
        return None if res is None else self._convert_select_result(res)

    def _convert_select_result(self, res):
        return res

    def _debug_close_connection(self):
        PgDb.plpy.connection = None

