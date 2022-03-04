from modules.db.pg import PgDb
from modules.db.pg_cassandra import PgDbCassandra
import modules.db.pg as pg
import modules.db.wrap as wrap
import modules.common_mod.const as const


class StatisticFunc(PgDbCassandra):
    ''' Statistic Functions of db scheme git999_log '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)


class LogFunc(PgDbCassandra):
    ''' Log Functions of db scheme git999_log '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    def _execute(self, plan_id, var_list, id_project = 0):
        res = super()._execute(plan_id, var_list, id_project, fix_log_on_expt = False)
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
        def local(self, record_type, id_project, node, node_table, description):
            return ('select git999_log.debug($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node, node_table, description, autocommit = autocommit)

    def log_info(self, record_type, id_project, node = None, node_table = None, description = '', autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, record_type, id_project, node, node_table, description):
            return ('select git999_log.info($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node, node_table, description, autocommit = autocommit)

    def log_warn(self, record_type, id_project, node = None, node_table = None, description = '', autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, record_type, id_project, node, node_table, description):
            return ('select git999_log.warn($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node, node_table, description, autocommit = autocommit)

    def log_error(self, record_type, id_project, node = None, node_table = None, description = '', autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, record_type, id_project, node, node_table, description):
            return ('select git999_log.error($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node, node_table, description, autocommit = autocommit)

    def log_fatal(self, record_type, id_project, node = None, node_table = None, description = '', autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, record_type, id_project, node, node_table, description):
            return ('select git999_log.fatal($1, $2, $3, $4, $5);',
                    ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"]
                   )
        return local(self, record_type, id_project, node, node_table, description, autocommit = autocommit)


class Cassandra_git999_log(LogFunc, StatisticFunc):
    ''' Functions of db scheme git999_log '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

