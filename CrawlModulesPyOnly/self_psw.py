def get_psw_vk_mtyurin():
    
    with open('C:\Temp\mypswvk.txt', 'r') as f:
        psw = f.read()

    return 'Bey'+psw+'00'

def get_psw_db_mtyurin():
    with open('C:\Temp\mypsw.txt', 'r') as f:
        psw = f.read()
    return 'Fdt'+psw+'00'

def get_api_hash_for_telegram():
    with open('C:\Temp\myapihashtg.txt', 'r') as f:
        psw = f.read()
    return '3f97ef9fde04'+psw+'37d03c5'

  
