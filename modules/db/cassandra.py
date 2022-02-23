import modules.common_mod.const as const
import modules.crawling.date as date

import modules.db.pg as pg
from modules.db.pg import execute_with_select_plan, select_with_select_plan
from modules.db.pg_cassandra import PgDbCassandra
from modules.db.git010_dict import Cassandra_git010_dict
from modules.db.git200_crawl import Cassandra_git200_crawl
from modules.db.git300_scrap import Cassandra_git300_scrap
from modules.db.git430_ner import Cassandra_git430_ner
from modules.db.git999_log import Cassandra_git999_log

if const.PY_ENVIRONMENT: 
    GD = None

class Cassandra(PgDbCassandra):
    def __init__(self, plpy, GD, **kwargs):
        super().__init__(plpy, GD)
        self.git010_dict  = Cassandra_git010_dict(**kwargs)
        self.git200_crawl = Cassandra_git200_crawl(**kwargs)
        self.git300_scrap = Cassandra_git300_scrap(**kwargs)
        self.git430_ner   = Cassandra_git430_ner(**kwargs)
        self.git999_log   = Cassandra_git999_log(**kwargs)

        if pg.gvars is not None and not pg.gvars.initialized:
            self._initialize_gvars()
        

    def _initialize_gvars(self):
        if not pg.gvars.initialized:
            pg.gvars.set('VK_SOURCE_ID', self.git010_dict.get_www_source_id('vk'))
            pg.gvars.set('TG_SOURCE_ID', self.git010_dict.get_www_source_id('tg'))

            ner_ent_types = {}
            for i in self.git430_ner.ent_type_select_all():
                ner_ent_types[i['name']] = { 'id': i['id'], 'not_entity': i['not_entity'] }
            pg.gvars.set('NER_ENT_TYPES', ner_ent_types)

            pg.gvars.initialize()

if __name__ == "__main__":
    if const.PY_ENVIRONMENT:
        import ModulesPyOnly.plpyemul as plpyemul
        plpy = plpyemul.get_plpy()    

    cass_db = Cassandra(plpy, GD)

    #cass_db.git999_log.log_trace('TEST new PY structure', 0, 'No description')
    #cass_db.git200_crawl.update_sn_num_subscribers(3, '50369640', 83497, True, '502', autocommit = False)
    cass_db.git200_crawl.update_sn_num_subscribers(3, '50369640', 83497, True, broken_status_code = '502', autocommit = True)

    f=1
