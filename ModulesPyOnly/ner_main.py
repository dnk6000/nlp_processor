import Modules.Common.const as const
import Modules.Common.pginterface as pginterface
import Modules.Parsing.processor as processor


####################################################
####### begin: for PY environment only #############
ID_PROJECT_main = 10

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
TG_SOURCE_ID = gvars.get('TG_SOURCE_ID')
VK_SOURCE_ID = gvars.get('VK_SOURCE_ID')

cass_db = pginterface.MainDB(plpy, GD)
need_stop_cheker = pginterface.NeedStopChecker(cass_db, ID_PROJECT_main, 'ner_recognize', state = 'off')

dp = processor.NerProcessor(db = cass_db,
                id_project = ID_PROJECT_main,
                id_www_source = TG_SOURCE_ID,
                need_stop_cheker = need_stop_cheker,
                debug_mode = DEBUG_MODE,
                msg_func = plpy.notice,
                portion_size = 10)

dp.debug_num_portions = 1

dp.process()
pass


