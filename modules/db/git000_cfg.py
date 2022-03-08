import modules.common_mod.const as const
import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git000_cfg(PgDbCassandra):
    ''' Functions of db scheme git000_cfg '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    def get_project_params(self, id_project):
        @wrap.select_with_query_plan_0
        def local(self, id_project):
            return (''' SELECT * FROM git000_cfg.get_project_params($1) ''', 
                    ["dmn.git_pk"])
        return local(self, id_project, str_to_date_conv_fields = ['date_deep'], decimal_to_float_conv_fields = ['requests_delay_sec'])

    @wrap.select_with_query_plan_0
    def get_proxy_project(self, id_project):
        return (''' SELECT * FROM git000_cfg.get_proxy_project($1) ''', 
                ["dmn.git_pk"])

    def create_project(self, 
                       id_project, 
                       name = '', 
                       full_name = '', 
                       date_deep = const.EMPTY_DATE, 
                       recrawl_days_post = 0, 
                       recrawl_days_reply = 0, 
                       requests_delay_sec = 0):
        @wrap.execute_with_query_plan
        def local(self, id_project, name, full_name, date_deep, recrawl_days_post, recrawl_days_reply, requests_delay_sec):
            return (''' SELECT * FROM git000_cfg.create_project($1, $2, $3, $4, $5, $6, $7) ''', 
                    ["dmn.git_pk", "dmn.git_string", "dmn.git_string", "dmn.git_datetime", "dmn.git_integer", "dmn.git_integer", "dmn.git_numeric"])
        return local(self, id_project, name, full_name, date_deep, recrawl_days_post, recrawl_days_reply, requests_delay_sec)

    @wrap.select_with_query_plan
    def need_stop_func(self, func_name, id_project):
        return (''' SELECT * FROM git000_cfg.need_stop_func($1, $2) ''', 
                ["dmn.git_string", "dmn.git_pk"])

    @wrap.execute_with_query_plan
    def set_config_param(self, key_name, key_value):
        return (''' SELECT * FROM git000_cfg.set_config_param($1, $2) ''', 
                ["dmn.git_string", "dmn.git_string"])


