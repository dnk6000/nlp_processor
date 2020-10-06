import psycopg2
import plpyemul

import time
from datetime import datetime

def get_psw_mtyurin():
    
    with open('C:\Temp\mypsw.txt', 'r') as f:
        psw = f.read()

    return 'Fdt'+psw+'00'

class CassandraDB():

    def __init__(self):
        self.SD = {}
        self.SD['plan_add_to_db_sn_accounts'] = None
        self.SD['update_sn_num_subscribers'] = None
        self.SD['add_to_db_data_text'] = None


    def add_to_db_sn_accounts(self, id_project, network, account_type, account_id, account_name, account_screen_name, account_closed):
        with plpy.subtransaction():
            if self.SD['plan_add_to_db_sn_accounts'] == None:
                self.SD['plan_add_to_db_sn_accounts'] = plpy.prepare('''
                        INSERT INTO git200_crawl.sn_accounts 
                            ( network, account_type, account_id, account_name, account_screen_name, account_closed, id_project ) 
                        VALUES ( $1, $2, $3, $4, $5, $6, $7 ) 
                        ON CONFLICT ON CONSTRAINT sn_accounts_id DO NOTHING
                        ''', 
                        #RETURNING id as NewID, network as nnetwork
                        ["dmn.enum_social_net", "dmn.enum_social_account_type", "integer", "text", "text", "boolean", "integer"])

            res = plpy.execute(self.SD['plan_add_to_db_sn_accounts'], [network, account_type, account_id, account_name, account_screen_name, account_closed, id_project])

        #plpy.commit() #commit is not necessary when using construction 'with'

    def update_sn_num_subscribers(self, network, id_project, account_id, number_subscribers):
        with plpy.subtransaction():
            if self.SD['update_sn_num_subscribers'] == None:
                self.SD['update_sn_num_subscribers'] = plpy.prepare('''
                                                             UPDATE git200_crawl.sn_accounts 
                                                             SET number_subscribers = $1  
                                                             WHERE  account_id = $2 
                                                             AND network = $3 
                                                             AND id_project = $4
                                                             ''', 
                                                         ["integer", "integer", "dmn.enum_social_net", "integer"]
                                                        )

            res = plpy.execute(self.SD['update_sn_num_subscribers'], [number_subscribers, account_id, network, id_project])


    def add_to_db_data_text(self, 
                            url, 
                            content, 
                            gid_data_html, 
                            content_header, 
                            content_date, 
                            id_project, 
                            sn_network, 
                            sn_id, 
                            sn_post_id, 
                            sn_post_parent_id
                            ):
        with plpy.subtransaction():
            if self.SD['add_to_db_data_text'] == None:
                self.SD['add_to_db_data_text'] = plpy.prepare('''
                                                       INSERT INTO git300_scrap.data_text ( 
                                                       source, content, gid_data_html, content_header, 
                                                       content_date, id_project, sn_network, sn_id, sn_post_id, sn_post_parent_id 
                                                        ) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 );
                                                             ''', 
                                                         ["text", "text", "text", "text", 
                                                          "datetime", "integer", "dmn.enum_social_net", "integer", "integer", "integer"]
                                                        )

            res = plpy.execute(self.SD['add_to_db_data_text'], 
                               [url, content, gid_data_html, content_header, 
                                content_date, id_project, sn_network, sn_id, sn_post_id, sn_post_parent_id
                                ]
                               )

   
    def CloseConnection(self):

        plpy.connection.close()

    #def SelectGroupsID(self):
    #    self.cursor.execute("SELECT account_id \
    #                        FROM git200_crawl.sn_accounts")
    #    rows = self.cursor.fetchall()
    #    return [row[0] for row in rows]

    #def Select(self):
    #    self.cursor.execute("SELECT id, network, account_type, account_id, account_name, account_screen_name, account_closed \
    #                        FROM git200_crawl.sn_accounts")
    #    rows = self.cursor.fetchall()
    #    for row in rows:  
    #       print(row)
    #       print("\n")

class DBError(Exception):
    pass

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

if __name__ == "__main__":
    #print(get_psw_mtyurin())

    CassDB = CassandraDB()
    #CassDB.Connect()
    print("Database opened successfully")

    #CassDB.add_to_db_sn_accounts(0, "vk", "group", 883564356, "group1212121212", "Тест группа 1212121212", True)
    #CassDB.add_to_db_sn_accounts(0, "vk", "group", 893564356, "group1212121212", "Тест группа 1212121212", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "rferfer", "аааааааааа", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "erf", "бббббббб", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "vfvffrrrr", "ггггггггг", False)

    CassDB.update_sn_num_subscribers('vk', 0, 16758516, 333)
    CassDB.update_sn_num_subscribers('vk', 0, 16758516, 444)

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

    print("Record added")


    #CassDB.Select()

    #CassDB.CloseConnection()
    f=1

