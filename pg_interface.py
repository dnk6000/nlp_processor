import psycopg2
import const

try:
    import plpyemul
except:
    pass

import time
from datetime import datetime

def get_psw_mtyurin():
    
    with open('C:\Temp\mypsw.txt', 'r') as f:
        psw = f.read()

    return 'Fdt'+psw+'00'

class MainDB():

    def __init__(self):
        pass


    def update_sn_num_subscribers(self, sn_network, sn_id, num_subscribers, **kwargs):
        plan_id = 'plan_update_sn_num_subscribers'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''SELECT * FROM git200_crawl.set_sn_accounts_num_subscribers($1, $2, $3)''', 
                                ["dmn.git_pk", "dmn.git_bigint", "dmn.git_integer"])

            res = plpy.execute(SD[plan_id], [sn_network, sn_id, num_subscribers])

    def upsert_data_text(self, id_data_html, id_project, id_www_sources, content, content_header = '', content_date = const.EMPTY_DATE,
                                sn_id = None, sn_post_id = None, sn_post_parent_id = None, **kwargs):
        plan_id = 'plan_upsert_data_text'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''select git300_scrap.upsert_data_text($1, $2, $3, $4, $5, $6, $7, $8, $9)''', 
                ["git_pk","git_pk","git_pk","dmn.git_text","dmn.git_text","dmn.git_datetime","dmn.git_bigint","dmn.git_bigint","dmn.git_bigint"])

            res = plpy.execute(SD[plan_id],[id_data_html, id_project, id_www_sources, content, content_header, content_date,
                                            sn_id, sn_post_id, sn_post_parent_id])

            return res[0]

    def upsert_data_html(self, url, content, id_project, id_www_sources, **kwargs):
        plan_id = 'plan_upsert_data_html'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''SELECT * FROM git200_crawl.upsert_data_html($1, $2, $3, $4)''', 
                                            ["dmn.git_string","dmn.git_text","dmn.git_pk","dmn.git_pk"])

            res = plpy.execute(SD[plan_id],[url, content, id_project, id_www_sources])

            return res[0]

    def upsert_sn_accounts(self, id_www_sources, id_project, account_type, account_id, account_name,
                                 account_screen_name, account_closed, num_subscribers = None):
        plan_id = 'plan_upsert_sn_accounts'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''SELECT * FROM git200_crawl.upsert_sn_accounts($1, $2, $3, $4, $5, $6, $7, $8)''', 
                        ["dmn.git_pk", "dmn.git_pk", "dmn.git_string", "dmn.git_bigint", "dmn.git_string", "dmn.git_string", "dmn.git_boolean", "dmn.git_integer"])

            res = plpy.execute(SD[plan_id], [id_www_sources, id_project, account_type, account_id, account_name,
                                 account_screen_name, account_closed, num_subscribers])
            return res[0]

    def upsert_sn_activity(self, id_source, sn_id, last_date):
        plan_id = 'plan_upsert_sn_activity'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                pg_func = 'select git200_crawl.upsert_sn_activity($1, $2, $3);'

                SD[plan_id] = plpy.prepare(pg_func, ["dmn.git_pk","dmn.git_bigint","dmn.git_datetime"])

            res = plpy.execute(SD[plan_id], [id_source, sn_id, last_date])

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
                   WHERE id_project = $1
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

    #def Select(self):
    #    self.cursor.execute("SELECT id, network, account_type, account_id, account_name, account_screen_name, account_closed \
    #                        FROM git200_crawl.sn_accounts")
    #    rows = self.cursor.fetchall()
    #    for row in rows:  
    #       print(row)
    #       print("\n")
    pass

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
    #res = cass_db.select_groups_id(id_project = 5)
    #res2 = list(i['account_id'] for i in res)
    #res = CassDB.select_groups_id(0)

    res = cass_db.upsert_sn_accounts(3, 10, 'G', 112774, 'test TEST', 'test33', False, 112)

    #res = cass_db.get_www_source_id('vk')

    #res = cass_db.log_debug('Ошибка 555', 1, '555 Длинное описание')

    #res = cass_db.add_to_db_data_html(url = 'testurl', content = 'test131123', domain = 'vk', id_project = 5, sid = 1)
    #res = cass_db.add_to_db_data_html(url = 'https://vk.com/andrey_fursov', content = 'test123', domain = 'vk', id_project = 5)
    #res = cass_db.add_to_db_data_html(url = 'https://vk.com/andrey_fursov', content = 
    #                          '''
    #                          '<!DOCTYPE html>\n<html prefix="og: http://ogp.me/ns#" lang=\'ru\' dir=\'ltr\'>\n<head>\n<meta http-equiv="X-UA-Compatible" 
    #                          content="IE=edge" />\n<link rel="shortcut icon" href="/images/icons/favicons/fav_logo.ico?6"
    #                          '''
    #                          , domain = 'vk', id_project = 5)
    a = 1

    #res = cass_db.add_to_db_sn_accounts(0, "vk", "group", 6274356, '13-14 Февраля ♥ Valentine\'s days @ Jesus in Furs ♥ Презентация новой коллекции', "Тест группа 1212121212", True)
    #CassDB.add_to_db_sn_accounts(0, "vk", "group", 893564356, "group1212121212", "Тест группа 1212121212", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "rferfer", "аааааааааа", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "erf", "бббббббб", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "vfvffrrrr", "ггггггггг", False)

    #CassDB.update_sn_num_subscribers('vk', 0, 24328516, 333)
    #CassDB.update_sn_num_subscribers('vk', 0, 16758516, 444)

    #CassDB.add_to_db_data_text( 
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

