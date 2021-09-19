import Modules.Common.const as const
import Modules.Common.pginterface as pginterface

import Modules.CrawlingScrapy.spiders as spiders
from Modules.CrawlingScrapy.spiders.tripadv import TripAdvisorSpider
from Modules.CrawlingScrapy.spiders.tripadv import LemmatizeTripAdvisor


####################################################
####### begin: for PY environment only #############
step_name = 'debug'
step_name = 'process'
step_name = 'lemmatize'

if const.PY_ENVIRONMENT:
    import ModulesPyOnly.plpyemul as plpyemul
    plpy = plpyemul.get_plpy()    
####### end: for PY environment only #############
####################################################


from Modules.Common.globvars import GlobVars
if const.PY_ENVIRONMENT: 
    GD = None
else: 
    GD = {}
gvars = GlobVars(GD)

DEBUG_MODE = True


def clear_tables():
    tables = [
        'git010_dict.trip_advisor'
        ]
    for t in tables:
        plpy.notice('Delete table {}'.format(t))
        cass_db.clear_table(t)


cass_db = pginterface.MainDB(plpy, GD)
#need_stop_cheker = pginterface.NeedStopChecker(cass_db, ID_PROJECT_main, 'ner_recognize', state = 'off')

if step_name == 'debug':
    #clear_tables()
    pass

if step_name == 'lemmatize':
    LemmatizeTripAdvisor(cass_db)
    pass


if step_name == 'process':
    ta_spider = TripAdvisorSpider
    ta_spider.db = cass_db

    #scraper = spiders.RunSpider(TripAdvisorSpider)
    scraper = spiders.RunSpider(ta_spider)
    scraper.run_spiders()


