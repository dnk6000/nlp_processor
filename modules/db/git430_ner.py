from modules.db.pg import execute_with_select_plan, select_with_select_plan
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git430_ner(PgDbCassandra):
    ''' Functions of db scheme git010_dict '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    @select_with_select_plan
    def ent_type_select_all(self):
        return ('select * from git430_ner.ent_type_select_all();', 
                [])



