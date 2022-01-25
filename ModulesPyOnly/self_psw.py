import modules.common_mod.const as const

PSWFOLDER = '\\.py_cass\\'

def get_psw_vk_mtyurin():
    with open(const.HOME+PSWFOLDER+'mypswvk.txt', 'r') as f:
        psw = f.read()

    return 'Cov'+psw+'001'

def get_psw_db_mtyurin():
    with open(const.HOME+PSWFOLDER+'mypsw.txt', 'r') as f:
        psw = f.read()
    return 'Fdt'+psw+'00'

def get_api_hash_for_telegram():
    with open(const.HOME+PSWFOLDER+'myapihashtg.txt', 'r') as f:
        psw = f.read()
    return '3f97ef9fde04'+psw+'37d03c5'

  
