import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git200_crawl(PgDbCassandra):
    ''' Functions of db scheme git200_crawl '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    @wrap.execute_with_query_plan
    def update_sn_num_subscribers(self, id_www_source, account_id, num_subscribers, is_broken = False, broken_status_code = '', autocommit = True, **kwargs):
        return ('''SELECT * FROM git200_crawl.set_sn_accounts_num_subscribers($1, $2, $3, $4, $5)''', 
                                ["dmn.git_pk", "dmn.git_sn_id", "dmn.git_integer", "dmn.git_boolean", "dmn.git_string_32"])

    def upsert_data_html(self, url, content, id_project, id_www_sources, **kwargs):
        plan_id = 'plan_upsert_data_html'
        with self.plpy.subtransaction():
            if not self._is_plan_exist(plan_id):
                gvars.GD[plan_id] = self.plpy.prepare('''SELECT * FROM git200_crawl.upsert_data_html($1, $2, $3, $4)''', 
                                            ["dmn.git_string","dmn.git_text","dmn.git_pk","dmn.git_pk"])

            res = self._execute(plan_id,[url, content, id_project, id_www_sources], id_project)

        self.plpy.commit()
        return None if res is None else res[0]
