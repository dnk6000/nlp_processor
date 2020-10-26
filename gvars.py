''' Global variables. 
    Store variables in global dictionary GD, similar GD in plpy 
'''

'''
VK_SOURCE_ID - id VK in table www_sources
'''

import const

if not const.PG_ENVIRONMENT:
    GD = dict()

def set(key, value):
    GD[key] = value

def get(key):
    if not key in GD:
        GD[key] = None
    return GD[key]

set('VK_SOURCE_ID', 3)