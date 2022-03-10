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
        def local(self, www_source_name):
            return (''' SELECT git010_dict.get_www_sources_id($1) ''', 
                    ["text"])
        res = local(self, www_source_name)
        return res['get_www_sources_id']

    @wrap.execute_with_query_plan
    def upsert_trip_advisor(self, name, name_lemma, name2, address, category_str, longitude, latitude, url):
        return (''' SELECT * FROM git010_dict.upsert_trip_advisor($1, $2, $3, $4, $5, $6, $7, $8) ''', 
                ["dmn.git_string", "dmn.git_string", "dmn.git_string", "dmn.git_string", "dmn.git_string", 
                             "dmn.git_double", "dmn.git_double", "dmn.git_string"])

