import modules.common_mod.const as const
import modules.crawling.date as date

import modules.db.pg as pg
from modules.db.pg import execute_with_select_plan
from modules.db.pg_cassandra import PgDbCassandra
from modules.db.git010_dict import Cassandra_git010_dict
from modules.db.git430_ner import Cassandra_git430_ner

if const.PY_ENVIRONMENT: 
    GD = None

class Cassandra(PgDbCassandra):
    def __init__(self, plpy, GD, **kwargs):
        super().__init__(plpy, GD)
        self.git010_dict = Cassandra_git010_dict(**kwargs)
        self.git430_ner  = Cassandra_git430_ner(**kwargs)

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

    cass_db._is_plan_exist('111')
    cass_db.git430_ner.ent_type_select_all()
    cass_db.git430_ner.ent_type_select_all()

    f=1
