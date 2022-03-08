import modules.common_mod.const as const

import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git300_scrap(PgDbCassandra):
    ''' Functions of db scheme git300_scrap '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    def upsert_data_text(self, id_data_html, id_project, id_www_sources, content, content_header = '', content_date = const.EMPTY_DATE,
                                sn_id = None, sn_post_id = None, sn_post_parent_id = None, autocommit = True, **kwargs):
        @wrap.execute_with_query_plan_0
        def local(self, id_data_html, id_project, id_www_sources, content, content_header, content_date,
                                sn_id, sn_post_id, sn_post_parent_id, **kwargs):
            return ('''select git300_scrap.upsert_data_text($1, $2, $3, $4, $5, $6, $7, $8, $9)''', 
                    ["dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_text","dmn.git_text","dmn.git_datetime","dmn.git_sn_id","dmn.git_sn_id","dmn.git_sn_id"])
        res = local(self, id_data_html, id_project, id_www_sources, content, content_header, content_date,
                                sn_id, sn_post_id, sn_post_parent_id, autocommit = autocommit, **kwargs)
        return res

    @wrap.select_with_query_plan
    def data_text_select_unprocess(self, id_www_source, id_project, number_records = 100):
        return (''' select * from git300_scrap.data_text_select_unprocess($1, $2, $3); ''', 
                ["dmn.git_pk","dmn.git_pk","dmn.git_integer"])

    @wrap.execute_with_query_plan
    def data_text_set_is_process(self, id):
        return (''' SELECT * FROM git300_scrap.data_text_set_is_process($1) ''', 
                ["dmn.git_pk"])
