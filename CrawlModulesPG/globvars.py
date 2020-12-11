''' Global variables. 
    Store variables in global dictionary GD, similar GD in plpy 
'''

'''
VK_SOURCE_ID - id VK in table www_sources
'''

import CrawlModulesPG.const as const

GDpy = dict()

class GlobVars:
    def __init__(self, GD):
        if GD == None:
            self.GD = GDpy
        else:
            self.GD = GD
        self.initialized = 'gvars_init' in self.GD

    def initialize(self):
        self.GD['gvars_init'] = True
        self.initialized = True

    def set(self, key, value):
        self.GD[key] = value

    def get(self, key):
        if not key in self.GD:
            self.GD[key] = None
        return self.GD[key]



