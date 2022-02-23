from modules.db.pg import execute_with_select_plan, select_with_select_plan
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git200_crawl(PgDbCassandra):
    ''' Functions of db scheme git200_crawl '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    @execute_with_select_plan
    def update_sn_num_subscribers(self, id_www_source, account_id, num_subscribers, is_broken = False, broken_status_code = '', autocommit = True, **kwargs):
        return ('''SELECT * FROM git200_crawl.set_sn_accounts_num_subscribers($1, $2, $3, $4, $5)''', 
                                ["dmn.git_pk", "dmn.git_sn_id", "dmn.git_integer", "dmn.git_boolean", "dmn.git_string_32"])

