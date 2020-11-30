import psycopg2
import const
import exceptions

try:
    import plpyemul
except:
    pass

def get_psw_mtyurin():
    
    with open('C:\Temp\mypsw.txt', 'r') as f:
        psw = f.read()

    return 'Fdt'+psw+'00'

class MainDB():

    def __init__(self):
        self.db_error_counter = 0
        self.db_error_limit = 10  #number errors in a row (=подряд)

    def _check_db_error_limit(self, _exception):
        if _exception == None:
            self.db_error_counter = 0
        else:
            self.db_error_counter += 1
            if self.db_error_counter >= self.db_error_limit:
                plpy.notice('Limit of write errors in the database reached! (pg_interface.py)')
                raise _exception

    def _execute(self, plan_id, var_list, id_project = 0):
        try:
            res = plpy.execute(SD[plan_id], var_list)
            self._check_db_error_limit(None)
            return res
        except Exception as e:
            self.log_error(const.CW_RESULT_TYPE_DB_ERROR, id_project, exceptions.get_err_description(e, plan_id = plan_id, var_list = var_list))
            self._check_db_error_limit(e)
            return None



    def update_sn_num_subscribers(self, sn_network, sn_id, num_subscribers, is_broken = False, broken_status_code = '', **kwargs):
        plan_id = 'plan_update_sn_num_subscribers'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''SELECT * FROM git200_crawl.set_sn_accounts_num_subscribers($1, $2, $3, $4, $5)''', 
                                ["dmn.git_pk", "dmn.git_sn_id", "dmn.git_integer", "dmn.git_boolean", "dmn.git_string_32"])

            res = plpy.execute(SD[plan_id], [sn_network, sn_id, num_subscribers, is_broken, broken_status_code])

    def upsert_data_text(self, id_data_html, id_project, id_www_sources, content, content_header = '', content_date = const.EMPTY_DATE,
                                sn_id = None, sn_post_id = None, sn_post_parent_id = None, **kwargs):
        plan_id = 'plan_upsert_data_text'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''select git300_scrap.upsert_data_text($1, $2, $3, $4, $5, $6, $7, $8, $9)''', 
                ["dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_text","dmn.git_text","dmn.git_datetime","dmn.git_sn_id","dmn.git_sn_id","dmn.git_sn_id"])

            res = self._execute(plan_id, [id_data_html, id_project, id_www_sources, content, content_header, content_date,
                                                sn_id, sn_post_id, sn_post_parent_id], id_project)

            return None if res == None else res[0]

    def upsert_data_html(self, url, content, id_project, id_www_sources, **kwargs):
        plan_id = 'plan_upsert_data_html'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''SELECT * FROM git200_crawl.upsert_data_html($1, $2, $3, $4)''', 
                                            ["dmn.git_string","dmn.git_text","dmn.git_pk","dmn.git_pk"])

            res = self._execute(plan_id,[url, content, id_project, id_www_sources], id_project)

            return None if res == None else res[0]

    def upsert_sn_accounts(self, id_www_sources, id_project, account_type, account_id, account_name,
                                 account_screen_name, account_closed, num_subscribers = None):
        plan_id = 'plan_upsert_sn_accounts'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''SELECT * FROM git200_crawl.upsert_sn_accounts($1, $2, $3, $4, $5, $6, $7, $8)''', 
                        ["dmn.git_pk", "dmn.git_pk", "dmn.git_string_1", "dmn.git_sn_id", "dmn.git_string", "dmn.git_string", "dmn.git_boolean", "dmn.git_integer"])

            res = plpy.execute(SD[plan_id], [id_www_sources, id_project, account_type, account_id, account_name,
                                 account_screen_name, account_closed, num_subscribers])
            return res[0]

    def upsert_sn_activity(self, id_source, id_project, sn_id, sn_post_id, last_date, upd_date):
        plan_id = 'plan_upsert_sn_activity'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                pg_func = 'select git200_crawl.upsert_sn_activity($1, $2, $3, $4, $5, $6);'

                SD[plan_id] = plpy.prepare(pg_func, ["dmn.git_pk","dmn.git_pk","dmn.git_sn_id","dmn.git_sn_id","dmn.git_datetime","dmn.git_datetime"])

            res = plpy.execute(SD[plan_id], [id_source, id_project, sn_id, sn_post_id, last_date, upd_date])

    def get_www_source_id(self, www_source_name):
        ''' result syntax: res[0]['get_www_sources_id'] '''
        plan_id = 'plan_get_www_source_id'
        if not plan_id in SD or SD[plan_id] == None:
            SD[plan_id] = plpy.prepare(
                '''
                SELECT git010_dict.get_www_sources_id($1) 
                ''', 
                ["text"])

        res = plpy.execute(SD[plan_id], [www_source_name])
        return convert_select_result(res)

    def select_groups_id(self, id_project):
        plan_id = 'plan_select_groups_id'
        if not plan_id in SD or SD[plan_id] == None:
            SD[plan_id] = plpy.prepare(
                '''
                SELECT id, account_id 
                   FROM git200_crawl.sn_accounts
                   WHERE id_project = $1 AND NOT account_closed
                ''', 
                ["integer"])

        res = plpy.execute(SD[plan_id], [id_project])
        return convert_select_result(res)

    def log_trace(self, record_type, id_project, description):
        self._log_write(const.CW_LOG_LEVEL_TRACE, record_type, id_project, description)

    def log_debug(self, record_type, id_project, description):
        self._log_write(const.CW_LOG_LEVEL_DEBUG, record_type, id_project, description)

    def log_info(self, record_type, id_project, description):
        self._log_write(const.CW_LOG_LEVEL_INFOcord_type, id_project, description)

    def log_warn(self, record_type, id_project, description):
        self._log_write(const.CW_LOG_LEVEL_WARN, record_type, id_project, description)

    def log_error(self, record_type, id_project, description):
        self._log_write(const.CW_LOG_LEVEL_ERROR, record_type, id_project, description)

    def log_fatal(self, record_type, id_project, description):
        self._log_write(const.CW_LOG_LEVEL_FATAL, record_type, id_project, description)

    def _log_write(self, log_level, record_type, id_project, description):
        plan_id = 'plan_log_write_'+const.CW_LOG_LEVEL_FUNC[log_level]
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                pg_func = 'select git999_log.write($1, $2, $3, $4, $5);'
                pg_func = pg_func.replace('write', const.CW_LOG_LEVEL_FUNC[log_level])

                SD[plan_id] = plpy.prepare(pg_func, ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"])

            res = plpy.execute(SD[plan_id], [record_type, id_project, None, None, description])

    def queue_generate(self, id_www_source, id_project):
        plan_id = 'plan_queue_generate'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                pg_func = 'select * from git200_crawl.queue_generate($1, $2);'

                SD[plan_id] = plpy.prepare(pg_func, ["dmn.git_pk","dmn.git_pk"])

            res = plpy.execute(SD[plan_id], [id_www_source, id_project])
        return res

    def queue_update(self, id_queue, is_process = None, date_start_process = None, date_end_process = None, 
                           date_deferred = None, attempts_counter = None):
        plan_id = 'plan_queue_update'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                pg_func = 'select * from git200_crawl.queue_update($1, $2, $3, $4, $5, $6);'

                SD[plan_id] = plpy.prepare(pg_func, ["dmn.git_pk","dmn.git_boolean","dmn.git_datetime","dmn.git_datetime","dmn.git_datetime","dmn.git_integer"])

            res = plpy.execute(SD[plan_id], [id_queue, is_process, date_start_process, date_end_process, date_deferred, attempts_counter])
        return res
    
    def queue_select(self, id_www_source, id_project, number_records = 10):
        plan_id = 'plan_queue_select'
        if not plan_id in SD or SD[plan_id] == None:
            pg_func = 'select * from git200_crawl.queue_select($1, $2, $3);'
            SD[plan_id] = plpy.prepare(pg_func, ["dmn.git_pk","dmn.git_pk","dmn.git_integer"])

        res = plpy.execute(SD[plan_id], [id_www_source, id_project, number_records])

        return res

    def clear_table_by_project(self, table_name, id_project):
        with plpy.subtransaction():
            pg_func = 'delete from {} where id_project = {};'.format(table_name, id_project)
            plpy.execute(pg_func)
    
    def get_sn_activity(self, id_www_sources, id_project, sn_id, recrawl_days_post):
        plan_id = 'plan_get_sn_activity'
        if not plan_id in SD or SD[plan_id] == None:
            SD[plan_id] = plpy.prepare(
                '''
                SELECT * FROM git200_crawl.get_sn_activity($1, $2, $3, $4)
                ''', 
                ["dmn.git_pk", "dmn.git_pk", "dmn.git_sn_id", "dmn.git_integer"])

        res = plpy.execute(SD[plan_id], [id_www_sources, id_project, sn_id, recrawl_days_post])
        return convert_select_result(res)

    def get_project_params(self, id_project):
        plan_id = 'plan_get_project_params'
        if not plan_id in SD or SD[plan_id] == None:
            SD[plan_id] = plpy.prepare(
                '''
                SELECT * FROM git000_cfg.get_project_params($1)
                ''', 
                ["dmn.git_pk"])

        res = plpy.execute(SD[plan_id], [id_project])
        return convert_select_result(res)

    def need_stop_func(self, func_name, id_project):
        plan_id = 'plan_need_stop_func'
        if not plan_id in SD or SD[plan_id] == None:
            SD[plan_id] = plpy.prepare(
                '''
                SELECT * FROM git000_cfg.need_stop_func($1, $2)
                ''', 
                ["dmn.git_string", "dmn.git_pk"])

        res = plpy.execute(SD[plan_id], [func_name, id_project])
        return convert_select_result(res)

    def set_config_param(self, key_name, key_value):
        plan_id = 'plan_set_config_param'
        if not plan_id in SD or SD[plan_id] == None:
            SD[plan_id] = plpy.prepare(
                '''
                SELECT * FROM git000_cfg.set_config_param($1, $2)
                ''', 
                ["dmn.git_string", "dmn.git_string"])

        res = plpy.execute(SD[plan_id], [key_name, key_value])
        return convert_select_result(res)


    #def Select(self):
    #    self.cursor.execute("SELECT id, network, account_type, account_id, account_name, account_screen_name, account_closed \
    #                        FROM git200_crawl.sn_accounts")
    #    rows = self.cursor.fetchall()
    #    for row in rows:  
    #       print(row)
    #       print("\n")
    pass

class NeedStopChecker:
    def __init__(self, cass_db, id_project, func_name, state = None):
        self.cass_db = cass_db
        self.id_project = id_project
        self.func_name = func_name
        if state == 'off':
            self.set_stop_off()
        elif state == 'on':
            self.set_stop_on()

    def set_stop_off(self):
        self.cass_db.set_config_param('stop_func_'+self.func_name, '')

    def set_stop_on(self):
        self.cass_db.set_config_param('stop_func_'+self.func_name, str(self.id_project))

    def need_stop(self):
        res = self.cass_db.need_stop_func(self, func_name, id_project)
        if res[0].result:
            raise exceptions.UserInterruptByDB()

def convert_select_result(res, num_fields = 1):
    '''in pgree environment need another processing!
    '''
    if num_fields == 1:
        #return [row[0] for row in res]
        return res
        

# plpy object creating, this code cannot be executed in the PGree environment  
try:
    cassandra_db_conn_par = {
        'database': 'cassandra_new', 
        'host'   : '192.168.60.46', 
        'port': '5432', 
        'user': 'm.tyurin', 
        'password': get_psw_mtyurin()
    }
    plpy = plpyemul.PlPy(**cassandra_db_conn_par)
except:
    pass

SD = {}

if __name__ == "__main__":
    #print(get_psw_mtyurin())

    cass_db = MainDB()
    #CassDB.Connect()
    print("Database opened successfully")

    #cass_db.clear_table_by_project('git200_crawl.data_html', 6)
    #res = cass_db.select_groups_id(id_project = 5)
    #res2 = list(i['account_id'] for i in res)
    #res = CassDB.select_groups_id(0)

    #res = cass_db.upsert_sn_accounts(3, 10, 'G', 112774, 'test TEST', 'test33', False, 112)

    #res = cass_db.get_www_source_id('vk')

    #res = cass_db.log_debug('Ошибка 555', 1, '555 Длинное описание')

    #res = cass_db.queue_generate(3, 5)  #res[0]['Success']
    #res = cass_db.queue_update(1, is_process = True)  #res[0]['Success']
    #import scraper
    #import datetime

    #res = cass_db.queue_update(1, date_deferred = scraper.date_to_str(datetime.datetime.now()))  #res[0]['Success']
    #res = cass_db.queue_update(1, attempts_counter = 11)  #res[0]['Success']
    #
    #res = cass_db.get_sn_activity(3, 6, 16758516, 90)
    #res = cass_db.get_project_params(0)
    res = cass_db.need_stop_func('crawl_wall', str(7))  #res[0]['result']
    res = cass_db.set_config_param('stop_func_crawl_wall', str(0))  #res[0]['result']
    res = cass_db.need_stop_func('crawl_wall', str(7))  #res[0]['result']
    a = 1

    #res = cass_db.add_to_db_sn_accounts(0, "vk", "group", 6274356, '13-14 Февраля ♥ Valentine\'s days @ Jesus in Furs ♥ Презентация новой коллекции', "Тест группа 1212121212", True)

    #CassDB.update_sn_num_subscribers('vk', 0, 16758516, 444)

    #CassDB.upsert_data_text( 
    #                        url = 'test', 
    #                        content = 'test text', 
    #                        gid_data_html = 0, 
    #                        content_header = 'test head', 
    #                        content_date = datetime.today(), 
    #                        id_project = 0, 
    #                        sn_network = 'vk', 
    #                        sn_id = 111, 
    #                        sn_post_id = 222, 
    #                        sn_post_parent_id = 333)

    #print(CassDB.select_groups_id(0))

    print("Record added")


    #CassDB.Select()

    #CassDB.CloseConnection()
    f=1

