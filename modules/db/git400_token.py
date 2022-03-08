import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git400_token(PgDbCassandra):
    ''' Functions of db scheme git400_token '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    @wrap.execute_with_query_plan
    def token_upsert_sentence(self, id_www_source, id_project, id_data_text, sentences_array):
        return (''' SELECT * FROM git400_token.upsert_sentence($1, $2, $3, $4) ''', 
                ["dmn.git_pk", "dmn.git_pk", "dmn.git_pk", "dmn.git_text []"])
