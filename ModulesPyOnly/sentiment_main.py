from datetime import time as timedt

import modules.common_mod.const as const
import modules.common_mod.pginterface as pginterface
import modules.common_mod.pauser as pauser
import modules.common_mod.jobs as jobs
import modules.common_mod.exceptions as exceptions
import modules.parsing.processor as processor

from modules.common_mod.globvars import GlobVars
if const.PY_ENVIRONMENT: 
    GD = None
else: 
    GD = {}
gvars = GlobVars(GD)

####################################################
####### begin: for PY environment only #############
job_id = 101
#job_id = None

step_name = 'debug'
step_name = 'process'
step_name = 'process_shedule'

#will be apply for debug only: when job_id is None
ID_PROJECT_main = 10
SOURCE_ID = 4
DEBUG_MODE = True
PORTION_SIZE = 100
DEBUG_NUM_PORTIONS = 100

if const.PY_ENVIRONMENT:
    import ModulesPyOnly.plpyemul as plpyemul
    plpy = plpyemul.get_plpy()    
####### end: for PY environment only #############
####################################################



def clear_tables_by_project(id_project):
    tables = [
        'git700_rate.sentence',
        'git700_rate.text',
        'git400_token.sentence'
        ]
    for t in tables:
        plpy.notice('Delete table {} by project {}'.format(t,id_project))
        cass_db.clear_table_by_project(t, id_project)



try:

    cass_db = pginterface.MainDB(plpy, GD)

    job = jobs.JobManager(id_job = job_id, db = cass_db)

    need_stop_cheker = pginterface.NeedStopChecker.get_need_stop_cheker(job, cass_db, ID_PROJECT_main, 'tokenize')

    while job.get_next_step():

        if not job_id is None:
            step_params = job.get_step_params()
            step_name = step_params['step_name']
            ID_PROJECT_main = step_params['id_project']
            SOURCE_ID = step_params['id_source']
            PORTION_SIZE = step_params['portion_size']
            DEBUG_NUM_PORTIONS = step_params['debug_num_portions']
            DEBUG_MODE = step_params['debug_mode']

        cass_db.create_project(ID_PROJECT_main)

        print(f'step_name: {step_name} ID_PROJECT: {ID_PROJECT_main}    SOURCE_ID: {SOURCE_ID}    PORTION_SIZE: {PORTION_SIZE}     DEBUG_NUM_PORTIONS: {DEBUG_NUM_PORTIONS}')


        if step_name == 'debug':
            TG_SOURCE_ID = gvars.get('TG_SOURCE_ID')
            VK_SOURCE_ID = gvars.get('VK_SOURCE_ID')

            #clear_tables_by_project(ID_PROJECT_main)
            import sys
            sys.exit(0)


        if step_name == 'process':

            dp = processor.SentimentProcessor(db = cass_db,
                            id_project = ID_PROJECT_main,
                            id_www_source = SOURCE_ID,
                            need_stop_cheker = need_stop_cheker,
                            debug_mode = DEBUG_MODE,
                            msg_func = plpy.notice,
                            portion_size = PORTION_SIZE)

            dp.debug_num_portions = DEBUG_NUM_PORTIONS

            dp.process()
            del dp #to free memory

        if step_name == 'process_shedule':

            #VK_ID_PROJECT = 9
            #TG_ID_PROJECT = 10

            #PORTION_SIZE = 100
            #NUM_PORTIONS = 20

            #shedule_pauser = pauser.DayShedulePauser()
            #shedule_pauser.add_pause(timedt(9,0,0),timedt(23,59,59))

            #dp_vk = processor.SentimentProcessor(db = cass_db,
            #                id_project = VK_ID_PROJECT,
            #                id_www_source = VK_SOURCE_ID,
            #                need_stop_cheker = need_stop_cheker,
            #                debug_mode = DEBUG_MODE,
            #                msg_func = plpy.notice,
            #                portion_size = PORTION_SIZE)

            #dp_vk.debug_num_portions = NUM_PORTIONS

            #dp_tg = processor.SentimentProcessor(db = cass_db,
            #                id_project = TG_ID_PROJECT,
            #                id_www_source = TG_SOURCE_ID,
            #                need_stop_cheker = need_stop_cheker,
            #                debug_mode = DEBUG_MODE,
            #                msg_func = plpy.notice,
            #                portion_size = PORTION_SIZE)

            #dp_tg.debug_num_portions = NUM_PORTIONS

            #while True:
            #    shedule_pauser.sleep_if_need()
            #    dp_vk.process()
            #    shedule_pauser.sleep_if_need()
            #    dp_tg.process()
            pass

except exceptions.StopProcess:
    #its ok  maybe user stop process
    pass
except Exception as e: 
    cass_db.log_fatal('CriticalErr on sentiment_main', ID_PROJECT_main, description=exceptions.get_err_description(e))
    raise

