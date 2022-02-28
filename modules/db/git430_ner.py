import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git430_ner(PgDbCassandra):
    ''' Functions of db scheme git010_dict '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    @wrap.select_with_query_plan
    def ent_type_select_all(self):
        return ('select * from git430_ner.ent_type_select_all();', 
                [])



