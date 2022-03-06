import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git000_cfg(PgDbCassandra):
    ''' Functions of db scheme git000_cfg '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    
    def _11111111get_www_source_id(self, www_source_name):
        @wrap.select_with_query_plan_0
        def _get_www_source_id(self, www_source_name):
            return (''' SELECT git010_dict.get_www_sources_id($1) ''', 
                    ["text"])
        res = _get_www_source_id(self, www_source_name)
        return res['get_www_sources_id']

    def get_project_params(self, id_project):
        plan_id = 'plan_get_project_params'
        if not self._is_plan_exist(plan_id):
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git000_cfg.get_project_params($1)
                ''', 
                ["dmn.git_pk"])

        res = self.plpy.execute(gvars.GD[plan_id], [id_project])
        return convert_select_result(res, ['date_deep'], ['requests_delay_sec']) 

    def get_proxy_project(self, id_project):
        plan_id = 'plan_get_proxy_project'
        if not self._is_plan_exist(plan_id):
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git000_cfg.get_proxy_project($1)
                ''', 
                ["dmn.git_pk"])

        res = self.plpy.execute(gvars.GD[plan_id], [id_project])
        return convert_select_result(res) 

    def create_project(self, 
                       id_project, 
                       name = '', 
                       full_name = '', 
                       date_deep = const.EMPTY_DATE, 
                       recrawl_days_post = 0, 
                       recrawl_days_reply = 0, 
                       requests_delay_sec = 0):
        plan_id = 'plan_create_project'
        if not self._is_plan_exist(plan_id):
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git000_cfg.create_project($1, $2, $3, $4, $5, $6, $7)
                ''', 
                ["dmn.git_pk", "dmn.git_string", "dmn.git_string", "dmn.git_datetime", "dmn.git_integer", "dmn.git_integer", "dmn.git_numeric"])
        res = self.plpy.execute(gvars.GD[plan_id], [id_project, name, full_name, date_deep, recrawl_days_post, recrawl_days_reply, requests_delay_sec])
        self.plpy.commit()

    def need_stop_func(self, func_name, id_project):
        plan_id = 'plan_need_stop_func'
        if not self._is_plan_exist(plan_id):
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git000_cfg.need_stop_func($1, $2)
                ''', 
                ["dmn.git_string", "dmn.git_pk"])

        res = self.plpy.execute(gvars.GD[plan_id], [func_name, id_project])
        return convert_select_result(res)

    def set_config_param(self, key_name, key_value):
        plan_id = 'plan_set_config_param'
        if not self._is_plan_exist(plan_id):
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git000_cfg.set_config_param($1, $2)
                ''', 
                ["dmn.git_string", "dmn.git_string"])

        res = self.plpy.execute(gvars.GD[plan_id], [key_name, key_value])
        self.plpy.commit()
        return convert_select_result(res)


