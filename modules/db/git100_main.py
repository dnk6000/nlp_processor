import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class JobFunc(PgDbCassandra):
    ''' Job Functions of db scheme git100_main '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    @wrap.select_with_query_plan
    def job_read(self, id):
        return (''' SELECT * FROM git100_main.job_read($1) ''', 
                ["dmn.git_pk"])

    @wrap.execute_with_query_plan
    def job_turn_off(self, id):
        return (''' SELECT * FROM git100_main.job_turn_off($1) ''', 
                ["dmn.git_pk"])

    @wrap.select_with_query_plan
    def job_need_stop(self, id):
        return (''' SELECT * FROM git100_main.job_need_stop($1) ''', 
                ["dmn.git_pk"])


class Cassandra_git100_main(JobFunc):
    ''' Functions of db scheme git100_main '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

