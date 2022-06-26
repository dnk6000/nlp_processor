import modules.db.wrap as wrap
from modules.db.pg_cassandra import PgDbCassandra

class QueueFunc(PgDbCassandra):
    ''' Statistic Functions of db scheme git999_log '''
    
    def __init__(self, **kwargs):
        ''' initialized only from class Cassandra '''
        super().__init__(**kwargs)

    def queue_generate(self, id_www_source, id_project, min_num_subscribers = 0, max_num_subscribers = 99999999, id_process = 0, autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, id_www_source, id_project, min_num_subscribers, max_num_subscribers, id_process):
            return ('select * from git200_crawl.queue_generate($1, $2, $3, $4, $5);',
                    ["dmn.git_pk","dmn.git_pk","dmn.git_integer","dmn.git_integer","dmn.git_pk"])
        return local(self, id_www_source, id_project, min_num_subscribers, max_num_subscribers, id_process, autocommit=autocommit)

    def queue_update(self, id_queue,                is_process = None,    date_start_process = None, 
                           date_end_process = None, date_deferred = None, attempts_counter = None, 
                           autocommit = True):
        @wrap.execute_with_query_plan
        def local(self, id_queue, is_process, date_start_process, date_end_process, date_deferred, attempts_counter):
            return ('select * from git200_crawl.queue_update($1, $2, $3, $4, $5, $6);',
                    ["dmn.git_pk","dmn.git_boolean","dmn.git_datetime","dmn.git_datetime","dmn.git_datetime","dmn.git_integer"])
        return local(self, id_queue, is_process, date_start_process, date_end_process, date_deferred, attempts_counter, autocommit=autocommit)
    
    def queue_select(self, id_www_source, id_project, number_records = 10, id_process = 0):
        @wrap.select_with_query_plan
        def local(self, id_www_source, id_project, number_records, id_process):
            return ('select * from git200_crawl.queue_select($1, $2, $3, $4);',
                    ["dmn.git_pk","dmn.git_pk","dmn.git_integer","dmn.git_pk"])
        return local(self, id_www_source, id_project, number_records, id_process)

    def queue_delete(self, id_project, id_process = 0, autocommit = True):
        @wrap.execute_with_query_plan
        def local(self):
            return (f'delete from git200_crawl.queue where id_project = {id_project} AND id_process = {id_process};',
                    [])
        return local(self, autocommit = autocommit)



class Cassandra_git200_crawl(QueueFunc):
    ''' Functions of db scheme git200_crawl '''
    
    def __init__(self, db, **kwargs):
        ''' initialized only from class Cassandra '''
        self.db = db
        super().__init__(**kwargs)

    @wrap.execute_with_query_plan_0
    def upsert_data_html(self, url, content, id_project, id_www_sources):
        return ('''SELECT * FROM git200_crawl.upsert_data_html($1, $2, $3, $4)''',
                ["dmn.git_string","dmn.git_text","dmn.git_pk","dmn.git_pk"])


    def update_sn_num_subscribers(self, id_www_source=0, account_id='', num_subscribers=0, is_broken = False, broken_status_code = '', id_project = 0, autocommit = True, **kwargs):
        @wrap.execute_with_query_plan
        def local(self, id_www_source, account_id, num_subscribers, is_broken, broken_status_code, id_project):
            return ('''SELECT * FROM git200_crawl.set_sn_accounts_num_subscribers($1, $2, $3, $4, $5, $6)''', 
                                    ["dmn.git_pk", "dmn.git_sn_id", "dmn.git_integer", "dmn.git_boolean", "dmn.git_string_32", "dmn.git_pk"])
        return local(self, id_www_source, account_id, num_subscribers, is_broken, broken_status_code, id_project, autocommit = autocommit)

    def upsert_sn_accounts(self, id_www_sources=0,       id_project=0,         account_type='',      account_id='',          account_name='',
                                 account_screen_name='', account_closed=False, account_extra_1 = '', num_subscribers = None, parameters = '', 
                                 autocommit = True, **kvargs):
        @wrap.execute_with_query_plan_0
        def local(self, id_www_sources,      id_project,     account_type,    account_id,      account_name,
                        account_screen_name, account_closed, account_extra_1, num_subscribers, parameters):
            return ('''SELECT * FROM git200_crawl.upsert_sn_accounts($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)''', 
                        ["dmn.git_pk", "dmn.git_pk", "dmn.git_string_1", "dmn.git_sn_id", "dmn.git_string", 
                         "dmn.git_string", "dmn.git_boolean", "dmn.git_string", "dmn.git_integer", "dmn.git_string"])
        return local(self, id_www_sources,      id_project,     account_type,    account_id,      account_name,
                           account_screen_name, account_closed, account_extra_1, num_subscribers, parameters, 
                           autocommit = autocommit)


    def upsert_sn_activity(self, id_source=0, id_project=0, sn_id='', sn_post_id='', last_date=None, upd_date=None, autocommit = True, **kwargs):
        @wrap.execute_with_query_plan
        def local(self, id_source, id_project, sn_id, sn_post_id, last_date, upd_date):
            return ('select git200_crawl.upsert_sn_activity($1, $2, $3, $4, $5, $6);',
                ["dmn.git_pk","dmn.git_pk","dmn.git_sn_id","dmn.git_sn_id","dmn.git_datetime","dmn.git_datetime"])
        return local(self, id_source, id_project, sn_id, sn_post_id, last_date, upd_date, autocommit = autocommit)

    @wrap.select_with_query_plan
    def get_sn_activity(self, id_www_sources=0, id_project=0, sn_id='', recrawl_days_post=30, str_to_date_conv_fields = ['last_date', 'upd_date']):
        return ('''
                SELECT * FROM git200_crawl.get_sn_activity($1, $2, $3, $4)
                ''', 
                ["dmn.git_pk", "dmn.git_pk", "dmn.git_sn_id", "dmn.git_integer"])

    @wrap.execute_with_query_plan
    def set_sn_activity_fin_date(self, id_www_sources=0, id_project=0, sn_id='', fin_date=None):
        return ('''
                SELECT * FROM git200_crawl.set_sn_activity_fin_date($1, $2, $3, $4)
                ''', 
                ["dmn.git_pk", "dmn.git_pk", "dmn.git_sn_id", "dmn.git_datetime"])


    @wrap.select_with_query_plan
    def get_doubles_accounts(self, projects_arr: list):
        return ('select * from git200_crawl.get_doubles_accounts($1);',
                ["dmn.git_pk[]"])
