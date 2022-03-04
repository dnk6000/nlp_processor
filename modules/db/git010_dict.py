import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git010_dict(PgDbCassandra):
    ''' Functions of db scheme git010_dict '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    
    def get_www_source_id(self, www_source_name):
        @wrap.select_with_query_plan_0
        def _get_www_source_id(self, www_source_name):
            return (''' SELECT git010_dict.get_www_sources_id($1) ''', 
                    ["text"])
        res = _get_www_source_id(self, www_source_name)
        return res['get_www_sources_id']

