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

    @wrap.execute_with_query_plan
    def token_upsert_word(self, id_www_source, id_project, id_data_text, id_sentence, words_array, lemms_array, id_entities_array):
        return (''' SELECT * FROM git400_token.upsert_word($1, $2, $3, $4, $5, $6, $7) ''', 
                ["dmn.git_pk", "dmn.git_pk", "dmn.git_pk", "dmn.git_pk", "dmn.git_text []", "dmn.git_text []", "dmn.git_pk []"])
    
    def sentence_set_is_process(self, id, is_broken = False, id_broken_type = None, autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, id, is_broken, id_broken_type):
            return ('''SELECT * FROM git400_token.sentence_set_is_process($1, $2, $3)''', 
                    ["dmn.git_pk","dmn.git_boolean","dmn.git_pk"])
        return local(self, id, is_broken, id_broken_type, autocommit = autocommit)

    def sentence_select_unprocess(self, id_www_source, id_project, number_records = 100, debug_sentence_id = 0, debug_sentence_id_arr = [], autocommit = True):
        @wrap.select_with_query_plan
        def local(self, id_www_source, id_project, number_records, debug_sentence_id, debug_sentence_id_arr):
            return ('''select * from git400_token.sentence_select_unprocess($1, $2, $3, $4, $5);''', 
                    ["dmn.git_pk","dmn.git_pk","dmn.git_integer","dmn.git_pk","dmn.git_pk[]"])
        return local(self, id_www_source, id_project, number_records, debug_sentence_id, debug_sentence_id_arr, autocommit = autocommit)

