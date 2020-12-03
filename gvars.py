''' Global variables. 
    Store variables in global dictionary GD, similar GD in plpy 
'''

'''
VK_SOURCE_ID - id VK in table www_sources
'''

import const
import pg_interface

if not const.PG_ENVIRONMENT:
    GD = dict()

def set(key, value):
    GD[key] = value

def get(key):
    if not key in GD:
        GD[key] = None
    return GD[key]

def initialize():
    cass_db = pg_interface.MainDB()

    if not 'VK_SOURCE_ID' in GD:
        GD['VK_SOURCE_ID'] = cass_db.get_www_source_id('vk')

initialize()