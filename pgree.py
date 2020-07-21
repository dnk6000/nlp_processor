import psycopg2

def get_psw_mtyurin():
    
    with open('C:\Temp\mypsw.txt', 'r') as f:
        psw = f.read()

    return 'Fdt'+psw+'00'

class CassandraDB():

    def __init__( self, 
                  database = "cassandra", 
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

    def AddToBD_SocialNet(self, network, account_type, account_id, account_name, account_screen_name, account_closed):

        self.cursor.execute("INSERT INTO \"1_in_html_crowler_dirty_data\".social_net \
                ( network, account_type, account_id, account_name, account_screen_name, account_closed ) \
                VALUES ( %s, %s, %s, %s, %s, %s ) \
                ON CONFLICT ON CONSTRAINT social_net_netid DO NOTHING", 
                (network, account_type, account_id, account_name, account_screen_name, account_closed))

        self.connection.commit()

    def CloseConnection(self):

        self.connection.close()

    def Select(self):
        self.cursor.execute("SELECT id, network, account_type, account_id, account_name, account_screen_name, account_closed \
                            FROM \"1_in_html_crowler_dirty_data\".social_net")
        rows = self.cursor.fetchall()
        for row in rows:  
           print(row)
           print("\n")


if __name__ == "__main__":
    #print(get_psw_mtyurin())

    CassDB = CassandraDB(password=get_psw_mtyurin())
    CassDB.Connect()
    print("Database opened successfully")

    CassDB.AddToBD_SocialNet("vk", "group", 123456, "group11_22", "Тест группа 1", True)
    CassDB.AddToBD_SocialNet("vk", "group", 123456, "rferfer", "аааааааааа", True)
    CassDB.AddToBD_SocialNet("vk", "group", 123456, "erf", "бббббббб", True)
    CassDB.AddToBD_SocialNet("vk", "group", 123456, "vfvffrrrr", "ггггггггг", False)

    print("Record added")


    CassDB.Select()

    CassDB.CloseConnection()

