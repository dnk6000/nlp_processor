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

    def clear_table_by_project(self, table_name, id_project, autocommit = True):
        @wrap.execute_with_query_plan
        def local(self):
            return (f'delete from {table_name} where id_project = {id_project};',
                    [])
        return local(self, autocommit = autocommit)
    
    def clear_table(self, table_name, autocommit = True):
        @wrap.execute_with_query_plan
        def local(self):
            return (f'delete from {table_name};',
                    [])
        return local(self, autocommit = autocommit)
    

