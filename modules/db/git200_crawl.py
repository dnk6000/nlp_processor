import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git200_crawl(PgDbCassandra):
    ''' Functions of db scheme git200_crawl '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    def update_sn_num_subscribers(self, id_www_source, account_id, num_subscribers, is_broken = False, broken_status_code = '', autocommit = True, **kwargs):
        @wrap.execute_with_query_plan
        def local(self, id_www_source, account_id, num_subscribers, is_broken, broken_status_code):
            return ('''SELECT * FROM git200_crawl.set_sn_accounts_num_subscribers($1, $2, $3, $4, $5)''', 
                                    ["dmn.git_pk", "dmn.git_sn_id", "dmn.git_integer", "dmn.git_boolean", "dmn.git_string_32"])
        return local(self, id_www_source, account_id, num_subscribers, is_broken, broken_status_code, autocommit = autocommit)

    @wrap.execute_with_query_plan_0
    def upsert_data_html(self, url, content, id_project, id_www_sources, **kwargs):
        return ('''SELECT * FROM git200_crawl.upsert_data_html($1, $2, $3, $4)''',
                ["dmn.git_string","dmn.git_text","dmn.git_pk","dmn.git_pk"])

    def upsert_sn_accounts(self, id_www_sources,      id_project,     account_type,         account_id,             account_name,
                                 account_screen_name, account_closed, account_extra_1 = '', num_subscribers = None, parameters = '', 
                                 autocommit = True, **kwargs):
        @wrap.execute_with_query_plan_0
        def local(self, id_www_sources,      id_project,     account_type,    account_id,      account_name,
                        account_screen_name, account_closed, account_extra_1, num_subscribers, parameters, 
                        **kwargs):
            return ('''SELECT * FROM git200_crawl.upsert_sn_accounts($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)''', 
                        ["dmn.git_pk", "dmn.git_pk", "dmn.git_string_1", "dmn.git_sn_id", "dmn.git_string", 
                         "dmn.git_string", "dmn.git_boolean", "dmn.git_string", "dmn.git_integer", "dmn.git_string"])
        return local(self, id_www_sources,      id_project,     account_type,    account_id,      account_name,
                           account_screen_name, account_closed, account_extra_1, num_subscribers, parameters, 
                           autocommit = autocommit)

    @wrap.execute_with_query_plan
    def upsert_sn_activity(self, id_source, id_project, sn_id, sn_post_id, last_date, upd_date, **kwargs):
        return ('select git200_crawl.upsert_sn_activity($1, $2, $3, $4, $5, $6);',
                ["dmn.git_pk","dmn.git_pk","dmn.git_sn_id","dmn.git_sn_id","dmn.git_datetime","dmn.git_datetime"])


