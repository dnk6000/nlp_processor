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
        pass        


    def add_to_db_sn_accounts(self, id_project, network, account_type, account_id, account_name, account_screen_name, account_closed):

        successfully = False
        i = 0

        while not successfully and i < 3:
            #try:
                #plan = plpy.execute("prepare plan_add_to_db_sn_accounts as INSERT INTO git200_crawl.sn_accounts \
                #        ( network, account_type, account_id, account_name, account_screen_name, account_closed, id_project ) \
                #        VALUES ( $1, $2, $3, $4, $5, $6, $7 ) \
                #        ON CONFLICT ON CONSTRAINT sn_accounts_id DO NOTHING", 
                #        ["text", "text", "integer", "text", "text", "boolean", "integer"])

                plan = plpy.prepare("INSERT INTO git200_crawl.sn_accounts \
                        ( network, account_type, account_id, account_name, account_screen_name, account_closed, id_project ) \
                        VALUES ( $1, $2, $3, $4, $5, $6, $7 ) \
                        ON CONFLICT ON CONSTRAINT sn_accounts_id DO NOTHING", 
                        ["dmn.enum_social_net", "dmn.enum_social_account_type", "integer", "text", "text", "boolean", "integer"])

                plpy.execute(plan, [network, account_type, account_id, account_name, account_screen_name, account_closed, id_project])

                #plan = plpy.execute('execute plan_add_to_db_sn_accounts (%s)' % ', '.join(
                #                    ["'"+str(network)+"'", 
                #                     "'"+str(account_type)+"'", 
                #                     str(account_id), 
                #                     "'"+str(account_name)+"'", 
                #                     "'"+str(account_screen_name)+"'", 
                #                     str(account_closed), 
                #                     str(id_project)
                #                    ]
                #                    )
                #                   )

                plpy.commit()

                successfully = True

            #except:
            #    i += 1

            #    if i >= 3:
            #        raise DBError('Ошибка записи в БД')
            #    else:
            #        print('Ошибка записи в БД !!! Попытка '+str(i))
            #        time.sleep(60)

    #def update_sn_num_subscribers(self, network, id_project, account_id, number_subscribers):

    #    successfully = False
    #    i = 0
        
    #    while not successfully and i < 3:
    #        try:
    #            self.cursor.execute(
    #                "UPDATE git200_crawl.sn_accounts \
    #                 SET number_subscribers = %s  \
    #                 WHERE  account_id = %s \
    #                 AND network = %s \
    #                 AND id_project = %s", 
    #                (number_subscribers, account_id, network, id_project))

    #            self.connection.commit()

    #            successfully = True

    #        except:
    #            i += 1

    #            if i >= 3:
    #                raise DBError('Ошибка записи в БД git200_crawl.sn_accounts')
    #            else:
    #                print('Ошибка записи в БД !!! Попытка '+str(i))
    #                time.sleep(60)

    #def add_to_db_data_text(self, 
    #                        url, 
    #                        content, 
    #                        gid_data_html, 
    #                        content_header, 
    #                        content_date, 
    #                        id_project, 
    #                        sn_network, 
    #                        sn_id, 
    #                        sn_post_id, 
    #                        sn_post_parent_id
    #                        ):

    #    successfully = False
    #    i = 0

    #    while not successfully and i < 3:
    #        try:
    #            self.cursor.execute("INSERT INTO git300_scrap.data_text ( \
    #                                    source, content, gid_data_html, content_header, \
    #                                    content_date, id_project, sn_network, sn_id, sn_post_id, sn_post_parent_id \
    #                                    ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );", 
    #                    (url, content, gid_data_html, content_header, content_date, id_project, sn_network, sn_id, sn_post_id, sn_post_parent_id))

    #            self.connection.commit()

    #            successfully = True

    #        except:
    #            i += 1

    #            if i >= 3:
    #                raise DBError('Ошибка записи в БД git300_scrap.data_text')
    #            else:
    #                print('Ошибка записи в БД git300_scrap.data_text !!! Попытка '+str(i))
    #                time.sleep(60)

   
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

    CassDB.add_to_db_sn_accounts(0, "vk", "group", 1212121212, "group1212121212", "Тест группа 1212121212", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "rferfer", "аааааааааа", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "erf", "бббббббб", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "vfvffrrrr", "ггггггггг", False)

    #CassDB.update_sn_num_subscribers('vk', 0, 16758516, 333)

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

    CassDB.CloseConnection()

