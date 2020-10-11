import psycopg2

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

    def add_to_db_sn_accounts(self, id_project, network, account_type, account_id, account_name, account_screen_name, account_closed):
        plan_id = 'plan_add_to_db_sn_accounts'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''
                        INSERT INTO git200_crawl.sn_accounts 
                            ( network, account_type, account_id, account_name, account_screen_name, account_closed, id_project ) 
                        VALUES ( $1, $2, $3, $4, $5, $6, $7 ) 
                        ON CONFLICT ON CONSTRAINT sn_accounts_id DO NOTHING
                        ''', 
                        #RETURNING id as NewID, network as nnetwork
                        ["dmn.enum_social_net", "dmn.enum_social_account_type", "integer", "text", "text", "boolean", "integer"])

            res = plpy.execute(SD[plan_id], [network, account_type, account_id, account_name, account_screen_name, account_closed, id_project])
        #plpy.commit() #commit is not necessary when using construction 'with'
        pass

    def update_sn_num_subscribers(self, sn_network, id_project, sn_id, number_subscribers, **kwargs):
        plan_id = 'plan_update_sn_num_subscribers'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''
                                    UPDATE git200_crawl.sn_accounts 
                                    SET number_subscribers = $1  
                                    WHERE  account_id = $2 
                                    AND network = $3 
                                    AND id_project = $4
                                    ''', 
                                ["integer", "integer", "dmn.enum_social_net", "integer"]
                              )

            res = plpy.execute(SD[plan_id], [number_subscribers, sn_id, sn_network, id_project])


    def add_to_db_data_text(self, url, content, gid_data_html, content_header, content_date, 
                                  id_project, sn_network, sn_id, sn_post_id, sn_post_parent_id, sid, **kwargs):
        plan_id = 'plan_add_to_db_data_text'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''
                                INSERT INTO git300_scrap.data_text ( 
                                source, content, gid_data_html, content_header, 
                                content_date, id_project, sn_network, sn_id, sn_post_id, sn_post_parent_id, sid 
                                ) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11 );
                                        ''', 
                                    ["text", "text", "text", "text", 
                                    "datetime", "integer", "dmn.enum_social_net", "integer", "integer", "integer", "integer"]
                              )

            res = plpy.execute(SD[plan_id], 
                               [url, content, gid_data_html, content_header, 
                                content_date, id_project, sn_network, sn_id, sn_post_id, sn_post_parent_id, sid
                               ]
                              )

    def add_to_db_data_html(self, url, content, domain, id_project, sid, **kwargs):
        plan_id = 'plan_add_to_db_data_html'
        with plpy.subtransaction():
            if not plan_id in SD or SD[plan_id] == None:
                SD[plan_id] = plpy.prepare('''
                        INSERT INTO git200_crawl.data_html 
                                (URL, CONTENT, DOMAIN, id_project, sid) 
                        VALUES ($1, $2, $3, $4, $5) 
                        ON CONFLICT ON CONSTRAINT data_html_hash_key DO NOTHING
                        RETURNING gid AS gid
                        ''', 
                ["text","text","text","integer"])
          
            res = plpy.execute(SD[plan_id],[url, content, domain, id_project, sid])

            return res
   
    def select_groups_id(self, id_project):
        plan_id = 'plan_select_groups_id'
        if not plan_id in SD or SD[plan_id] == None:
            SD[plan_id] = plpy.prepare(
                '''
                SELECT account_id 
                   FROM git200_crawl.sn_accounts
                   WHERE id_project = $1
                ''', 
                ["integer"])

        res = plpy.execute(SD[plan_id], [id_project])
        return convert_select_result(res)

    #def Select(self):
    #    self.cursor.execute("SELECT id, network, account_type, account_id, account_name, account_screen_name, account_closed \
    #                        FROM git200_crawl.sn_accounts")
    #    rows = self.cursor.fetchall()
    #    for row in rows:  
    #       print(row)
    #       print("\n")

class DBError(Exception):
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

    res = cass_db.add_to_db_data_html(url = 'testurl', content = 'test131123', domain = 'vk', id_project = 5, sid = 1)
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

