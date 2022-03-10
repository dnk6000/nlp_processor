import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git700_rate(PgDbCassandra):
    ''' Functions of db scheme git700_rate '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    @wrap.execute_with_query_plan_0
    def sentiment_upsert_sentence(self, id_www_source, id_project, id_data_text, id_token_sentence, id_rating_type, autocommit = True, **kwargs):
        return (''' SELECT * FROM git700_rate.upsert_sentence($1, $2, $3, $4, $5) ''', 
                ["dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_pk"])

    @wrap.execute_with_query_plan_0
    def sentiment_upsert_text(self, id_www_source, id_project, id_data_text, id_rating_type, autocommit = True, **kwargs):
        return (''' SELECT * FROM git700_rate.upsert_text($1, $2, $3, $4) ''', 
                ["dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_pk"])

