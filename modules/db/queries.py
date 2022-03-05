import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_queries(PgDbCassandra):
    ''' Free queries to DB '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    
    @wrap.select_with_query_plan
    def select_groups_id(self, id_project):
        return ('''
                SELECT id, account_id 
                   FROM git200_crawl.sn_accounts
                   WHERE id_project = $1 AND NOT account_closed
                ''', 
                ["integer"])


