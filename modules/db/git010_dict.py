from modules.db.pg import execute_with_select_plan
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git010_dict(PgDbCassandra):
    ''' Functions of db scheme git010_dict '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    @execute_with_select_plan
    def _get_www_source_id(self, www_source_name):
        return (''' SELECT git010_dict.get_www_sources_id($1) ''', 
                ["text"])
    
    def get_www_source_id(self, www_source_name):
        res = self._get_www_source_id(www_source_name)
        return res[0]['get_www_sources_id']

