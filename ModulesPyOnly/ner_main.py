import Modules.Common.const as const
import Modules.Common.pginterface as pginterface
import Modules.Parsing.processor as processor


####################################################
####### begin: for PY environment only #############
step_name = 'process'
step_name = 'debug'
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


def clear_tables_by_project(id_project):
    tables = [
        'git430_ner.entity',
        'git400_token.word'
        ]
    for t in tables:
        plpy.notice('Delete table {} by project {}'.format(t,id_project))
        cass_db.clear_table_by_project(t, id_project)


cass_db = pginterface.MainDB(plpy, GD)
need_stop_cheker = pginterface.NeedStopChecker(cass_db, ID_PROJECT_main, 'ner_recognize', state = 'off')

TG_SOURCE_ID = gvars.get('TG_SOURCE_ID')
VK_SOURCE_ID = gvars.get('VK_SOURCE_ID')


if step_name == 'debug':
    import Modules.Parsing.token as token
    dp = token.SentenceTokenizer()
    txt_lst = [ '  я видимо здоровья дохуя я работаю\n При поддержке Золота Бородача  ', '  я видимо здоровья дохуя я работаю\nПри поддержке Золота Бородача  \n' ]
    res = dp.tokenize(txt_lst)


    #clear_tables_by_project(ID_PROJECT_main)
    #import sys
    #sys.exit(0)
    pass

if step_name == 'process':
    dp = processor.NerProcessor(db = cass_db,
                    id_project = ID_PROJECT_main,
                    id_www_source = TG_SOURCE_ID,
                    need_stop_cheker = need_stop_cheker,
                    debug_mode = DEBUG_MODE,
                    msg_func = plpy.notice,
                    portion_size = 10)

    dp.debug_num_portions = 2

    dp.process()
    pass


