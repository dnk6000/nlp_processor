from modules.db.pg import execute_with_select_plan, select_with_select_plan
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git300_scrap(PgDbCassandra):
    ''' Functions of db scheme git010_dict '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    @select_with_select_plan
    def _get_www_source_id(self, www_source_name):
        return (''' SELECT git010_dict.get_www_sources_id($1) ''', 
                ["text"])
    
    def get_www_source_id(self, www_source_name):
        res = self._get_www_source_id(www_source_name)
        return res[0]['get_www_sources_id']

    @execute_with_select_plan(res_first_row = True)
    def upsert_data_text(self, id_data_html, id_project, id_www_sources, content, content_header = '', content_date = const.EMPTY_DATE,
                                sn_id = None, sn_post_id = None, sn_post_parent_id = None, autocommit = True, **kwargs):
        return ('''select git300_scrap.upsert_data_text($1, $2, $3, $4, $5, $6, $7, $8, $9)''', 
                ["dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_text","dmn.git_text","dmn.git_datetime","dmn.git_sn_id","dmn.git_sn_id","dmn.git_sn_id"])
        
        #plan_id = 'plan_upsert_data_text'
        #with self.plpy.subtransaction():
        #    if not self._is_plan_exist(plan_id):
        #        gvars.GD[plan_id] = self.plpy.prepare('''select git300_scrap.upsert_data_text($1, $2, $3, $4, $5, $6, $7, $8, $9)''', 
        #        ["dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_text","dmn.git_text","dmn.git_datetime","dmn.git_sn_id","dmn.git_sn_id","dmn.git_sn_id"])
        #    res = self._execute(plan_id, [id_data_html, id_project, id_www_sources, content, content_header, content_date,
        #                                        sn_id, sn_post_id, sn_post_parent_id], id_project)
        #self.plpy.commit()
        #return None if res is None else res[0]
