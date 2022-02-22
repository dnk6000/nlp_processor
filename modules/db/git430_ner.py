from modules.db.pg import execute_with_select_plan
from modules.db.pg_cassandra import PgDbCassandra

class Cassandra_git430_ner(PgDbCassandra):
    ''' Functions of db scheme git010_dict '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    @execute_with_select_plan
    def ent_type_select_all(self):
        return ('select * from git430_ner.ent_type_select_all();', 
                [])

    def __ent_type_select_all(self):
        plan_id = 'plan_ent_type_select_all'
        if not self._is_plan_exist(plan_id):
            pg_func = 'select * from git430_ner.ent_type_select_all();'
            gvars.GD[plan_id] = self._prepare(pg_func, [])

        #res = self._execute(gvars.GD[plan_id], [])
        res = self._execute(plan_id, [])

        return self._convert_select_result(res)


