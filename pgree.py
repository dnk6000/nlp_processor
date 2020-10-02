import psycopg2

import time
from datetime import datetime

def get_psw_mtyurin():
    
    with open('C:\Temp\mypsw.txt', 'r') as f:
        psw = f.read()

    return 'Fdt'+psw+'00'

class CassandraDB():

    def __init__( self, 
                  database = "cassandra_new", 
                  host = "192.168.60.46", 
                  port = "5432",
                  user = "m.tyurin", 
                  password = ""):
        
        self.database = database 
        self.user = user 
        self.password =password 
        self.host = host 
        self.port = port


    def Connect(self):
    
        self.connection = psycopg2.connect(
          database = self.database, 
          user = self.user, 
          password = self.password, 
          host = self.host, 
          port = self.port
        )

        self.cursor = self.connection.cursor()

    def AddToBD_SocialNet(self, id_project, network, account_type, account_id, account_name, account_screen_name, account_closed):

        successfully = False
        i = 0

        while not successfully and i < 3:
            try:
                self.cursor.execute("INSERT INTO git200_crawl.sn_accounts \
                        ( network, account_type, account_id, account_name, account_screen_name, account_closed, id_project ) \
                        VALUES ( %s, %s, %s, %s, %s, %s, %s ) \
                        ON CONFLICT ON CONSTRAINT social_net_netid DO NOTHING", 
                        (network, account_type, account_id, account_name, account_screen_name, account_closed, id_project))

                self.connection.commit()

                successfully = True

            except:
                i += 1

                if i >= 3:
                    raise DBError('Ошибка записи в БД')
                else:
                    print('Ошибка записи в БД !!! Попытка '+str(i))
                    time.sleep(60)

    def update_sn_num_subscribers(self, network, id_project, account_id, number_subscribers):

        successfully = False
        i = 0
        
        while not successfully and i < 3:
            try:
                self.cursor.execute(
                    "UPDATE git200_crawl.sn_accounts \
                     SET number_subscribers = %s  \
                     WHERE  account_id = %s \
                     AND network = %s \
                     AND id_project = %s", 
                    (number_subscribers, account_id, network, id_project))

                self.connection.commit()

                successfully = True

            except:
                i += 1

                if i >= 3:
                    raise DBError('Ошибка записи в БД git200_crawl.sn_accounts')
                else:
                    print('Ошибка записи в БД !!! Попытка '+str(i))
                    time.sleep(60)

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

        successfully = False
        i = 0

        while not successfully and i < 3:
            try:
                self.cursor.execute("INSERT INTO git300_scrap.data_text ( \
                                        source, content, gid_data_html, content_header, \
                                        content_date, id_project, sn_network, sn_id, sn_post_id, sn_post_parent_id \
                                        ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );", 
                        (url, content, gid_data_html, content_header, content_date, id_project, sn_network, sn_id, sn_post_id, sn_post_parent_id))

                self.connection.commit()

                successfully = True

            except:
                i += 1

                if i >= 3:
                    raise DBError('Ошибка записи в БД git300_scrap.data_text')
                else:
                    print('Ошибка записи в БД git300_scrap.data_text !!! Попытка '+str(i))
                    time.sleep(60)

   
    def CloseConnection(self):

        self.connection.close()

    def SelectGroupsID(self):
        self.cursor.execute("SELECT account_id \
                            FROM git200_crawl.sn_accounts")
        rows = self.cursor.fetchall()
        return [row[0] for row in rows]

    def Select(self):
        self.cursor.execute("SELECT id, network, account_type, account_id, account_name, account_screen_name, account_closed \
                            FROM git200_crawl.sn_accounts")
        rows = self.cursor.fetchall()
        for row in rows:  
           print(row)
           print("\n")

class DBError(Exception):
    pass

if __name__ == "__main__":
    #print(get_psw_mtyurin())

    CassDB = CassandraDB(password=get_psw_mtyurin())
    CassDB.Connect()
    print("Database opened successfully")

    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "group11_22", "Тест группа 1", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "rferfer", "аааааааааа", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "erf", "бббббббб", True)
    #CassDB.AddToBD_SocialNet("vk", "group", 123456, "vfvffrrrr", "ггггггггг", False)

    #CassDB.update_sn_num_subscribers('vk', 0, 16758516, 333)

    CassDB.add_to_db_data_text( 
                            url = 'test', 
                            content = 'test text', 
                            gid_data_html = 0, 
                            content_header = 'test head', 
                            content_date = datetime.today(), 
                            id_project = 0, 
                            sn_network = 'vk', 
                            sn_id = 111, 
                            sn_post_id = 222, 
                            sn_post_parent_id = 333)

    print("Record added")


    CassDB.Select()

    CassDB.CloseConnection()

