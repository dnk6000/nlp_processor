from modules.db.pg import PgDb
from modules.db.pg_cassandra import PgDbCassandra
import modules.db.pg as pg
import modules.db.wrap as wrap
import modules.common_mod.const as const


class StatisticFunc(PgDbCassandra):
    ''' Statistic Functions of db scheme git999_log '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)


class LogFunc(PgDbCassandra):
    ''' Log Functions of db scheme git999_log '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    def _execute(self, plan_id, var_list, id_project = 0):
        exept = None
        res = PgDb._execute(self, plan_id, var_list, exept)
        if exept is not None:
            #raise
            pass
        return res

    def log_trace(self, record_type, id_project, node = None, node_table = None, description = '', autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, record_type, id_project, node, node_table, description):
            return ('select git999_log.trace($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node, node_table, description, autocommit = autocommit)


    def log_debug(self, record_type, id_project, node = None, node_table = None, description = '', autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, record_type, id_project, node, node_table, description, autocommit):
            return ('select git999_log.debug($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node = node, node_table = node_table, description = '', autocommit = autocommit)

    def log_info(self, record_type, id_project, node = None, node_table = None, description = '', autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, record_type, id_project, node, node_table, description, autocommit):
            return ('select git999_log.info($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node = node, node_table = node_table, description = '', autocommit = autocommit)

    def log_warn(self, record_type, id_project, node = None, node_table = None, description = '', autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, record_type, id_project, node, node_table, description, autocommit):
            return ('select git999_log.warn($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node = node, node_table = node_table, description = '', autocommit = autocommit)

    def log_error(self, record_type, id_project, node = None, node_table = None, description = '', autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, record_type, id_project, node, node_table, description, autocommit):
            return ('select git999_log.error($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node = node, node_table = node_table, description = '', autocommit = autocommit)

    def log_fatal(self, record_type, id_project, node = None, node_table = None, description = '', autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, record_type, id_project, node, node_table, description, autocommit):
            return ('select git999_log.fatal($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node = node, node_table = node_table, description = '', autocommit = autocommit)

    def OLD_log_write(self, log_level, record_type, id_project, description):
        plan_id = 'plan_log_write_'+const.LOG_LEVEL_FUNC[log_level]
        with self.subtransaction():
            if not self._is_plan_exist(plan_id):
                pg_func = 'select git999_log.write($1, $2, $3, $4, $5);'
                pg_func = pg_func.replace('write', const.LOG_LEVEL_FUNC[log_level])

                pg.gvars.GD[plan_id] = self._prepare(pg_func, ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"])

            res = self._execute(plan_id, [record_type, id_project, None, None, description])
        self.commit()

class Cassandra_git999_log(LogFunc, StatisticFunc):
    ''' Functions of db scheme git999_log '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

