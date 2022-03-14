import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git430_ner(PgDbCassandra):
    ''' Functions of db scheme git010_dict '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    @wrap.select_with_query_plan
    def ent_type_select_all(self):
        return ('select * from git430_ner.ent_type_select_all();', 
                [])

    @wrap.execute_with_query_plan_0
    def ent_type_insert(self, name, description):
        return ('''SELECT * FROM git430_ner.ent_type_insert($1, $2)''', 
                ["dmn.git_string","dmn.git_text"])

    @wrap.execute_with_query_plan
    def entity_upsert(self, id_www_source, id_project, id_data_text, id_sentence, id_ent_type, txt_lemm):
        return (''' SELECT * FROM git430_ner.entity_upsert($1, $2, $3, $4, $5, $6) ''', 
                ["dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_pk []","dmn.git_string []"])



