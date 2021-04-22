import Common.const as const
import Crawling.exceptions as exceptions
import Crawling.date as date

from Crawling.globvars import GlobVars
if const.PY_ENVIRONMENT: GD = None
gvars = None

class MainDB:

    def __init__(self, plpy, GD):
        self.db_error_counter = 0
        self.db_error_limit = 10  #number errors in a row (=подряд)
        
        self.plpy = plpy

        global gvars
        gvars = GlobVars(GD)
        self._initialize_gvars()


    def _initialize_gvars(self):
        if not gvars.initialized:
            gvars.set('VK_SOURCE_ID', self.get_www_source_id('vk'))
            gvars.set('TG_SOURCE_ID', self.get_www_source_id('tg'))
            gvars.initialize()

    def _check_db_error_limit(self, _exception):
        if _exception is None:
            self.db_error_counter = 0
        else:
            self.db_error_counter += 1
            if self.db_error_counter >= self.db_error_limit:
                self.plpy.notice('Limit of write errors in the database reached! (pg_interface.py)')
                raise _exception

    def _execute(self, plan_id, var_list, id_project = 0):
        try:
            res = self.plpy.execute(gvars.GD[plan_id], var_list)
            self._check_db_error_limit(None)
            return res
        except Exception as e:
            self.log_error(const.CW_RESULT_TYPE_DB_ERROR, id_project, exceptions.get_err_description(e, plan_id = plan_id, var_list = var_list))
            self._check_db_error_limit(e)
            return None



    def update_sn_num_subscribers(self, sn_network, sn_id, num_subscribers, is_broken = False, broken_status_code = '', **kwargs):
        plan_id = 'plan_update_sn_num_subscribers'
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                gvars.GD[plan_id] = self.plpy.prepare('''SELECT * FROM git200_crawl.set_sn_accounts_num_subscribers($1, $2, $3, $4, $5)''', 
                                ["dmn.git_pk", "dmn.git_sn_id", "dmn.git_integer", "dmn.git_boolean", "dmn.git_string_32"])

            res = self.plpy.execute(gvars.GD[plan_id], [sn_network, sn_id, num_subscribers, is_broken, broken_status_code])
        self.plpy.commit()

    def upsert_data_text(self, id_data_html, id_project, id_www_sources, content, content_header = '', content_date = const.EMPTY_DATE,
                                sn_id = None, sn_post_id = None, sn_post_parent_id = None, **kwargs):
        plan_id = 'plan_upsert_data_text'
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                gvars.GD[plan_id] = self.plpy.prepare('''select git300_scrap.upsert_data_text($1, $2, $3, $4, $5, $6, $7, $8, $9)''', 
                ["dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_text","dmn.git_text","dmn.git_datetime","dmn.git_sn_id","dmn.git_sn_id","dmn.git_sn_id"])
            res = self._execute(plan_id, [id_data_html, id_project, id_www_sources, content, content_header, content_date,
                                                sn_id, sn_post_id, sn_post_parent_id], id_project)
        self.plpy.commit()
        return None if res is None else res[0]

    def upsert_data_html(self, url, content, id_project, id_www_sources, **kwargs):
        plan_id = 'plan_upsert_data_html'
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                gvars.GD[plan_id] = self.plpy.prepare('''SELECT * FROM git200_crawl.upsert_data_html($1, $2, $3, $4)''', 
                                            ["dmn.git_string","dmn.git_text","dmn.git_pk","dmn.git_pk"])

            res = self._execute(plan_id,[url, content, id_project, id_www_sources], id_project)

        self.plpy.commit()
        return None if res is None else res[0]

    def upsert_sn_accounts(self, id_www_sources, id_project, account_type, account_id, account_name,
                                 account_screen_name, account_closed, account_extra_1 = '', num_subscribers = None, **kwargs):
        plan_id = 'plan_upsert_sn_accounts'
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                gvars.GD[plan_id] = self.plpy.prepare('''SELECT * FROM git200_crawl.upsert_sn_accounts($1, $2, $3, $4, $5, $6, $7, $8, $9)''', 
                        ["dmn.git_pk", "dmn.git_pk", "dmn.git_string_1", "dmn.git_sn_id", "dmn.git_string", "dmn.git_string", "dmn.git_boolean", "dmn.git_string", "dmn.git_integer"])

            res = self.plpy.execute(gvars.GD[plan_id], [id_www_sources, id_project, account_type, account_id, account_name,
                                 account_screen_name, account_closed, account_extra_1, num_subscribers])
        self.plpy.commit()
        return None if res is None else res[0]

    def upsert_sn_activity(self, id_source, id_project, sn_id, sn_post_id, last_date, upd_date, **kwargs):
        plan_id = 'plan_upsert_sn_activity'
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                pg_func = 'select git200_crawl.upsert_sn_activity($1, $2, $3, $4, $5, $6);'

                gvars.GD[plan_id] = self.plpy.prepare(pg_func, ["dmn.git_pk","dmn.git_pk","dmn.git_sn_id","dmn.git_sn_id","dmn.git_datetime","dmn.git_datetime"])

            res = self.plpy.execute(gvars.GD[plan_id], [id_source, id_project, sn_id, sn_post_id, last_date, upd_date])
        self.plpy.commit()

    def get_www_source_id(self, www_source_name):
        ''' result syntax: res[0]['get_www_sources_id'] '''
        plan_id = 'plan_get_www_source_id'
        if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT git010_dict.get_www_sources_id($1) 
                ''', 
                ["text"])

        res = self.plpy.execute(gvars.GD[plan_id], [www_source_name])
        return convert_select_result(res)[0]['get_www_sources_id']

    def select_groups_id(self, id_project):
        plan_id = 'plan_select_groups_id'
        if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT id, account_id 
                   FROM git200_crawl.sn_accounts
                   WHERE id_project = $1 AND NOT account_closed
                ''', 
                ["integer"])

        res = self.plpy.execute(gvars.GD[plan_id], [id_project])
        return convert_select_result(res)

    def log_trace(self, record_type, id_project, description):
        self._log_write(const.LOG_LEVEL_TRACE, record_type, id_project, description)

    def log_debug(self, record_type, id_project, description):
        self._log_write(const.LOG_LEVEL_DEBUG, record_type, id_project, description)

    def log_info(self, record_type, id_project, description):
        self._log_write(const.LOG_LEVEL_INFO, record_type, id_project, description)

    def log_warn(self, record_type, id_project, description):
        self._log_write(const.LOG_LEVEL_WARN, record_type, id_project, description)

    def log_error(self, record_type, id_project, description):
        self._log_write(const.LOG_LEVEL_ERROR, record_type, id_project, description)

    def log_fatal(self, record_type, id_project, description):
        self._log_write(const.LOG_LEVEL_FATAL, record_type, id_project, description)

    def _log_write(self, log_level, record_type, id_project, description):
        plan_id = 'plan_log_write_'+const.LOG_LEVEL_FUNC[log_level]
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                pg_func = 'select git999_log.write($1, $2, $3, $4, $5);'
                pg_func = pg_func.replace('write', const.LOG_LEVEL_FUNC[log_level])

                gvars.GD[plan_id] = self.plpy.prepare(pg_func, ["dmn.git_text","dmn.git_pk","dmn.git_pk","dmn.git_string","dmn.git_string"])

            res = self.plpy.execute(gvars.GD[plan_id], [record_type, id_project, None, None, description])
        self.plpy.commit()

    def queue_generate(self, id_www_source, id_project, min_num_subscribers = 0, max_num_subscribers = 99999999):
        plan_id = 'plan_queue_generate'
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                pg_func = 'select * from git200_crawl.queue_generate($1, $2, $3, $4);'

                gvars.GD[plan_id] = self.plpy.prepare(pg_func, ["dmn.git_pk","dmn.git_pk","dmn.git_integer","dmn.git_integer"])

            res = self.plpy.execute(gvars.GD[plan_id], [id_www_source, id_project, min_num_subscribers, max_num_subscribers])
        self.plpy.commit()
        return res

    def queue_update(self, id_queue, is_process = None, date_start_process = None, date_end_process = None, 
                           date_deferred = None, attempts_counter = None):
        plan_id = 'plan_queue_update'
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                pg_func = 'select * from git200_crawl.queue_update($1, $2, $3, $4, $5, $6);'

                gvars.GD[plan_id] = self.plpy.prepare(pg_func, ["dmn.git_pk","dmn.git_boolean","dmn.git_datetime","dmn.git_datetime","dmn.git_datetime","dmn.git_integer"])

            res = self.plpy.execute(gvars.GD[plan_id], [id_queue, is_process, date_start_process, date_end_process, date_deferred, attempts_counter])
        self.plpy.commit()
        return res
    
    def queue_select(self, id_www_source, id_project, number_records = 10):
        plan_id = 'plan_queue_select'
        if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
            pg_func = 'select * from git200_crawl.queue_select($1, $2, $3);'
            gvars.GD[plan_id] = self.plpy.prepare(pg_func, ["dmn.git_pk","dmn.git_pk","dmn.git_integer"])

        res = self.plpy.execute(gvars.GD[plan_id], [id_www_source, id_project, number_records])

        return res

    def clear_table_by_project(self, table_name, id_project):
        with self.plpy.subtransaction():
            pg_func = 'delete from {} where id_project = {};'.format(table_name, id_project)
            self.plpy.execute(pg_func)
        self.plpy.commit()
    
    def get_sn_activity(self, id_www_sources, id_project, sn_id, recrawl_days_post):
        plan_id = 'plan_get_sn_activity'
        if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git200_crawl.get_sn_activity($1, $2, $3, $4)
                ''', 
                ["dmn.git_pk", "dmn.git_pk", "dmn.git_sn_id", "dmn.git_integer"])

        res = self.plpy.execute(gvars.GD[plan_id], [id_www_sources, id_project, sn_id, recrawl_days_post])
        return convert_select_result(res, ['last_date', 'upd_date'])

    def get_project_params(self, id_project):
        plan_id = 'plan_get_project_params'
        if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git000_cfg.get_project_params($1)
                ''', 
                ["dmn.git_pk"])

        res = self.plpy.execute(gvars.GD[plan_id], [id_project])
        return convert_select_result(res, ['date_deep'], ['requests_delay_sec']) 

    def create_project(self, 
                       id_project, 
                       name = '', 
                       full_name = '', 
                       date_deep = const.EMPTY_DATE, 
                       recrawl_days_post = 0, 
                       recrawl_days_reply = 0, 
                       requests_delay_sec = 0):
        plan_id = 'plan_create_project'
        if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git000_cfg.create_project($1, $2, $3, $4, $5, $6, $7)
                ''', 
                ["dmn.git_pk", "dmn.git_string", "dmn.git_string", "dmn.git_datetime", "dmn.git_integer", "dmn.git_integer", "dmn.git_numeric"])
        res = self.plpy.execute(gvars.GD[plan_id], [id_project, name, full_name, date_deep, recrawl_days_post, recrawl_days_reply, requests_delay_sec])
        self.plpy.commit()

    def need_stop_func(self, func_name, id_project):
        plan_id = 'plan_need_stop_func'
        if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git000_cfg.need_stop_func($1, $2)
                ''', 
                ["dmn.git_string", "dmn.git_pk"])

        res = self.plpy.execute(gvars.GD[plan_id], [func_name, id_project])
        return convert_select_result(res)

    def set_config_param(self, key_name, key_value):
        plan_id = 'plan_set_config_param'
        if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git000_cfg.set_config_param($1, $2)
                ''', 
                ["dmn.git_string", "dmn.git_string"])

        res = self.plpy.execute(gvars.GD[plan_id], [key_name, key_value])
        self.plpy.commit()
        return convert_select_result(res)

    def data_text_select_unprocess(self, id_www_source, id_project, number_records = 100):
        plan_id = 'plan_data_text_select_unprocess'
        if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
            pg_func = 'select * from git300_scrap.data_text_select_unprocess($1, $2, $3);'
            gvars.GD[plan_id] = self.plpy.prepare(pg_func, ["dmn.git_pk","dmn.git_pk","dmn.git_integer"])

        res = self.plpy.execute(gvars.GD[plan_id], [id_www_source, id_project, number_records])

        return convert_select_result(res)

    def data_text_set_is_process(self, id):
        plan_id = 'plan_data_text_set_is_process'
        if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
            gvars.GD[plan_id] = self.plpy.prepare(
                '''
                SELECT * FROM git300_scrap.data_text_set_is_process($1)
                ''', 
                ["dmn.git_pk"])

        res = self.plpy.execute(gvars.GD[plan_id], [id])
        self.plpy.commit()

    def token_upsert_sentence(self, id_www_source, id_project, id_data_text, sentences_array):
        plan_id = 'plan_token_upsert_sentence'
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                gvars.GD[plan_id] = self.plpy.prepare('''SELECT * FROM git400_token.upsert_sentence($1, $2, $3, $4)''', 
                                ["dmn.git_pk", "dmn.git_pk", "dmn.git_pk", "dmn.git_text []"])

            res = self.plpy.execute(gvars.GD[plan_id], [id_www_source, id_project, id_data_text, sentences_array])
        self.plpy.commit()

    def sentiment_upsert_sentence(self, id_www_source, id_project, id_data_text, id_token_sentence, id_rating_type, **kwargs):
        plan_id = 'plan_sentiment_upsert_sentence'
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                gvars.GD[plan_id] = self.plpy.prepare('''SELECT * FROM git450_sentim.upsert_sentence($1, $2, $3, $4, $5)''', 
                                            ["dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_pk"])

            res = self._execute(plan_id,[id_www_source, id_project, id_data_text, id_token_sentence, id_rating_type])

        self.plpy.commit()
        return None if res is None else res[0]

    def sentiment_upsert_text(self, id_www_source, id_project, id_data_text, id_rating_type, **kwargs):
        plan_id = 'plan_sentiment_upsert_text'
        with self.plpy.subtransaction():
            if not plan_id in gvars.GD or gvars.GD[plan_id] is None:
                gvars.GD[plan_id] = self.plpy.prepare('''SELECT * FROM git450_sentim.upsert_text($1, $2, $3, $4)''', 
                                            ["dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_pk","dmn.git_pk"])

            res = self._execute(plan_id,[id_www_source, id_project, id_data_text, id_rating_type])

        self.plpy.commit()
        return None if res is None else res[0]

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
        res = self.cass_db.need_stop_func(self.func_name, self.id_project)
        if res[0]['result']:
            raise exceptions.UserInterruptByDB()
        #else:  #DEBUG
        #    return self.func_name+' res:' + str(res[0]['result'])  #DEBUG

def convert_select_result(res, str_to_date_conv_fields = [], decimal_to_float_conv_fields = [], msg_func = None):

    #plpy in PG return date fields in str format, therefore, a conversion is required
    if const.PG_ENVIRONMENT and len(str_to_date_conv_fields) > 0:

        converter = date.StrToDate('%Y-%m-%d %H:%M:%S+.*')

        for field in str_to_date_conv_fields:
            for row in res:
                row[field] = converter.get_date(row[field], type_res = 'D')
                
    if len(decimal_to_float_conv_fields) > 0:
        for field in decimal_to_float_conv_fields:
            for row in res:
                row[field] = row[field].__float__()  #class Decimal to float

    #return [row[0] for row in res]
    return res
        

if __name__ == "__main__":
    import CrawlingPyOnly.self_psw as self_psw
    from Crawling.globvars import GlobVars
    if const.PY_ENVIRONMENT: 
        GD = None
    else: 
        GD = {}
    if const.PY_ENVIRONMENT:
        import CrawlingPyOnly.plpyemul as plpyemul

        def get_psw_db_mtyurin():
            with open('C:\Temp\mypsw.txt', 'r') as f:
                psw = f.read()
            return 'Fdt'+psw+'00'

        cassandra_db_conn_par = {
            'database': 'cassandra_new', 
            'host'   : '192.168.60.46', 
            'port': '5432', 
            'user': 'm.tyurin', 
            'password': self_psw.get_psw_db_mtyurin()
        }
        plpy = plpyemul.PlPy(**cassandra_db_conn_par)

    cass_db = MainDB(plpy, GD)
    #CassDB.Connect()
    print("Database opened successfully")

    #cass_db.clear_table_by_project('git200_crawl.data_html', 6)
    #res = cass_db.select_groups_id(id_project = 5)
    #res2 = list(i['account_id'] for i in res)
    #res = CassDB.select_groups_id(0)

    #res = cass_db.upsert_sn_accounts(3, 10, 'G', 112774, 'test TEST', 'test33', False, 112)

    #res = cass_db.get_www_source_id('vk')

    #res = cass_db.log_error('Ошибка 555', 7, '555 Длинное описание')
    res = cass_db.data_text_select_unprocess(4,10,15)

    #res = cass_db.queue_generate(3, 5)  #res[0]['Success']
    #res = cass_db.queue_update(1, is_process = True)  #res[0]['Success']
    #import scraper
    #import datetime

    #res = cass_db.queue_update(1, date_deferred = scraper.date_to_str(datetime.datetime.now()))  #res[0]['Success']
    #res = cass_db.queue_update(1, attempts_counter = 11)  #res[0]['Success']
    #
    #res = cass_db.get_sn_activity(3, 6, 16758516, 90)
    #res = cass_db.get_project_params(0)
    #res = cass_db.need_stop_func('crawl_wall', str(7))  #res[0]['result']
    #res = cass_db.set_config_param('stop_func_crawl_wall', str(0))  #res[0]['result']
    #res = cass_db.need_stop_func('crawl_wall', str(7))  #res[0]['result']
    
    #res = cass_db.upsert_data_html(url = 'TST', content = 'TEST 03/12/2020', id_project = 10, id_www_sources = 3)
    #print(res) #res['id_modified']
    
    a = 1

    #res = cass_db.add_to_db_sn_accounts(0, "vk", "group", 6274356, '13-14 Февраля ♥ Valentine\'s days @ Jesus in Furs ♥ Презентация новой коллекции', "Тест группа 1212121212", True)

    #CassDB.update_sn_num_subscribers('vk', 0, 16758516, 444)

    #cass_db.upsert_data_text( 
    #                        url = 'test', 
    #                        content = 'test text', 
    #                        gid_data_html = 0, 
    #                        content_header = 'test head', 
    #                        content_date = datetime.today(), 
    #                        id_project = 0, 
    #                        sn_network = 3, 
    #                        sn_id = 111, 
    #                        sn_post_id = 222, 
    #                        sn_post_parent_id = 333)

    #print(CassDB.select_groups_id(0))

    print("Record added")


    #CassDB.Select()

    #CassDB.CloseConnection()
    f=1

