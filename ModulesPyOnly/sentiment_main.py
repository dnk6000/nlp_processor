import modules.common_mod.const as const
import modules.common_mod.pginterface as pginterface
import modules.parsing.processor as processor
import modules.common_mod.pauser as pauser
from datetime import time as timedt


####################################################
####### begin: for PY environment only #############
step_name = 'debug'
step_name = 'process'
step_name = 'process_shedule'
ID_PROJECT_main = 10

if const.PY_ENVIRONMENT:
    import ModulesPyOnly.plpyemul as plpyemul
    plpy = plpyemul.get_plpy()    
####### end: for PY environment only #############
####################################################

from modules.common_mod.globvars import GlobVars
if const.PY_ENVIRONMENT: 
    GD = None
else: 
    GD = {}
gvars = GlobVars(GD)

DEBUG_MODE = True


def clear_tables_by_project(id_project):
    tables = [
        'git700_rate.sentence',
        'git700_rate.text',
        'git400_token.sentence'
        ]
    for t in tables:
        plpy.notice('Delete table {} by project {}'.format(t,id_project))
        cass_db.clear_table_by_project(t, id_project)

cass_db = pginterface.MainDB(plpy, GD)
need_stop_cheker = pginterface.NeedStopChecker(cass_db, 10, 'tokenize', state = 'off')

TG_SOURCE_ID = gvars.get('TG_SOURCE_ID')
VK_SOURCE_ID = gvars.get('VK_SOURCE_ID')


if step_name == 'debug':
    clear_tables_by_project(ID_PROJECT_main)
    import sys
    sys.exit(0)
    pass


if step_name == 'process':

    dp = processor.SentimentProcessor(db = cass_db,
                    id_project = ID_PROJECT_main,
                    id_www_source = TG_SOURCE_ID,
                    need_stop_cheker = need_stop_cheker,
                    debug_mode = DEBUG_MODE,
                    msg_func = plpy.notice,
                    portion_size = 100)

    dp.debug_num_portions = 200

    dp.process()
    pass

if step_name == 'process_shedule':

    VK_ID_PROJECT = 9
    TG_ID_PROJECT = 10

    PORTION_SIZE = 100
    NUM_PORTIONS = 20

    shedule_pauser = pauser.DayShedulePauser()
    shedule_pauser.add_pause(timedt(9,0,0),timedt(23,59,59))

    dp_vk = processor.SentimentProcessor(db = cass_db,
                    id_project = VK_ID_PROJECT,
                    id_www_source = VK_SOURCE_ID,
                    need_stop_cheker = need_stop_cheker,
                    debug_mode = DEBUG_MODE,
                    msg_func = plpy.notice,
                    portion_size = PORTION_SIZE)

    dp_vk.debug_num_portions = NUM_PORTIONS

    dp_tg = processor.SentimentProcessor(db = cass_db,
                    id_project = TG_ID_PROJECT,
                    id_www_source = TG_SOURCE_ID,
                    need_stop_cheker = need_stop_cheker,
                    debug_mode = DEBUG_MODE,
                    msg_func = plpy.notice,
                    portion_size = PORTION_SIZE)

    dp_tg.debug_num_portions = NUM_PORTIONS

    while True:
        shedule_pauser.sleep_if_need()
        dp_vk.process()
        shedule_pauser.sleep_if_need()
        dp_tg.process()
