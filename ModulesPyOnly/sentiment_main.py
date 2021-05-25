import Modules.Common.const as const
import Modules.Common.pginterface as pginterface
import Modules.Parsing.processor as processor


if const.PY_ENVIRONMENT:
    import ModulesPyOnly.plpyemul as plpyemul
    plpy = plpyemul.get_plpy()    
if const.PY_ENVIRONMENT: 
    GD = None
else: 
    GD = {}
from Modules.Common.globvars import GlobVars
gvars = GlobVars(GD)


cass_db = pginterface.MainDB(plpy, GD)
need_stop_cheker = pginterface.NeedStopChecker(cass_db, 10, 'tokenize', state = 'off')

dp = processor.SentimentProcessor(db = cass_db,
                id_project = 10,
                id_www_source = 4,
                need_stop_cheker = need_stop_cheker,
                debug_mode = True,
                msg_func = plpy.notice,
                portion_size = 100)

dp.debug_num_portions = 1

dp.process()
pass

