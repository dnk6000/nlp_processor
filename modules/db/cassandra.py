import modules.common_mod.const as const
import modules.common_mod.date as date

import modules.db.pg as pg
from modules.db.pg_cassandra import PgDbCassandra
from modules.db.queries      import Cassandra_queries
from modules.db.git000_cfg   import Cassandra_git000_cfg
from modules.db.git010_dict  import Cassandra_git010_dict
from modules.db.git200_crawl import Cassandra_git200_crawl
from modules.db.git300_scrap import Cassandra_git300_scrap
from modules.db.git400_token import Cassandra_git400_token
from modules.db.git430_ner   import Cassandra_git430_ner
from modules.db.git700_rate  import Cassandra_git700_rate
from modules.db.git999_log   import Cassandra_git999_log

if const.PY_ENVIRONMENT: 
    GD = None

class Cassandra(PgDbCassandra):
    def __init__(self, plpy, GD, **kwargs):
        super().__init__(plpy, GD)
        self.db = self
        self.git000_cfg   = Cassandra_git000_cfg  (db = self, **kwargs)
        self.git010_dict  = Cassandra_git010_dict (db = self, **kwargs)
        self.git200_crawl = Cassandra_git200_crawl(db = self, **kwargs)
        self.git300_scrap = Cassandra_git300_scrap(db = self, **kwargs)
        self.git400_token = Cassandra_git400_token(db = self, **kwargs)
        self.git430_ner   = Cassandra_git430_ner  (db = self, **kwargs)
        self.git700_rate  = Cassandra_git700_rate (db = self, **kwargs)
        self.git999_log   = Cassandra_git999_log  (db = self, **kwargs)
        self.query        = Cassandra_queries     (db = self, **kwargs)

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
    #import re
    #match = re.search(r'prepared statement \"(\w*)\" does not exist', 'prepared statement "py_plan_11" does not exist\n') 

    #if match:
    #    a=1

    #import sys
    #sys.exit(0)

    if const.PY_ENVIRONMENT:
        import ModulesPyOnly.plpyemul as plpyemul
        plpy = plpyemul.get_plpy()    

    cass_db = Cassandra(plpy, GD)

    #cass_db.git999_log.log_trace('TEST new PY structure', 0, 'No description', autocommit = True)
    #cass_db.git999_log.log_trace('TEST new PY structure', 0, description='No description', autocommit = True)
    #cass_db.commit()
    #cass_db.git999_log.log_debug('TEST new PY structure', 0, description='No description')
    #cass_db.git999_log.log_info('TEST new PY structure', 0, description='No description')
    #cass_db.git999_log.log_warn('TEST new PY structure', 0, description='No description')
    #cass_db.git999_log.log_error('TEST new PY structure', 0, description='No description')
    #cass_db.git999_log.log_fatal('TEST new PY structure', 0, description='No description')

    #cass_db.git300_scrap.upsert_data_text(11111, 1, 1, 'Test content', content_header = 'Test header', content_date = date.date_now_str(),
    #                            sn_id = 1, sn_post_id = 2, sn_post_parent_id = 3, autocommit = True)
    #cass_db.git200_crawl.update_sn_num_subscribers(3, '50369640', 83497, True, '502', autocommit = True)
    #cass_db.git200_crawl.update_sn_num_subscribers(3, '50369640', 83497, True, broken_status_code = '502', autocommit = True)
    #cass_db.git200_crawl.upsert_data_html('tst.ru', 'test content', 1, 1)
    #res = cass_db.git200_crawl.upsert_sn_accounts(1,      1,     'G',         1036240821,             'meduzalive',
    #                             'Медуза — LIVE', False, account_extra_1 = '', num_subscribers = 435435, parameters = '', 
    #                             autocommit = True)
    #res = cass_db.git200_crawl.upsert_sn_activity(1, 1, '50369640', 12, '2022.03.04', '2022.03.06')
    #res = cass_db.query.select_groups_id(10)
    #cass_db.query.clear_table_by_project('git200_crawl.queue', 10)
    #res = cass_db.git200_crawl.queue_generate(4, 10, min_num_subscribers = 15000, max_num_subscribers = 99999999, autocommit = True)
    #res = cass_db.git200_crawl.queue_update(id_queue=2135587, is_process = True,    date_start_process = '2022.03.04')
    #res = cass_db.git200_crawl.queue_select(4, id_project=10)
    #res = cass_db.git200_crawl.get_sn_activity(4, 10, '1117628569', recrawl_days_post=30, str_to_date_conv_fields = ['last_date', 'upd_date'])
    #res = cass_db.git200_crawl.set_sn_activity_fin_date(id_www_sources = 4, id_project = 1, sn_id = '1430295016', fin_date = '2022.03.05', autocommit = True)
    #res = cass_db.git000_cfg.get_proxy_project(1)
    #res = cass_db.git000_cfg.get_project_params(10)
    #res = cass_db.git000_cfg.create_project(1)
    #res = cass_db.git000_cfg.need_stop_func('crawl_wall',1)
    #res = cass_db.git000_cfg.set_config_param('stop_func_crawl_wall','234')

    #res = cass_db.git300_scrap.data_text_select_unprocess(4, 10, 10)
    #res = cass_db.git300_scrap.data_text_set_is_process(11947684)

    #res = cass_db.git400_token.token_upsert_sentence(1, 1, 1111, ['Test1', 'Test2 Sent'], autocommit = True)
    #res = cass_db.git700_rate.sentiment_upsert_sentence(1, 1, 2, 3, 4, autocommit = False)
    res = cass_db.git700_rate.sentiment_upsert_text(1, 1, 2, 3, autocommit = False)
    cass_db.commit()
    #print(str(res))
    #cass_db.git300_scrap.upsert_data_text(id_data_html, id_project, id_www_sources, content, content_header = '', content_date = const.EMPTY_DATE,
    #                            sn_id = None, sn_post_id = None, sn_post_parent_id = None, autocommit = True, **kwargs)
    f=1
