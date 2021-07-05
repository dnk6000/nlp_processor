import Modules.Common.const as const
import Modules.Common.pginterface as pginterface
import Modules.Parsing.processor as processor


####################################################
####### begin: for PY environment only #############
step_name = 'debug'
step_name = 'debug_sent_list'
step_name = 'process'
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
    #import re
    #regex = re.compile("(?:(?:https{0,1}|ftp|www):(//|\\\\\\\\)|www\\.|ftp\\.)",re.I)
    #testString = r'Hello www.google.com World http://yahoo.com  http:\\yahoo.com   http://www.yahoo.com' # fill this in
    #matchArray = regex.match(testString)
    ## the matchArray variable contains the list of matches
    #re.finditer(r'(?:(?:https?|ftp|file):(//|\\)|www\.|ftp\.)', r'Hello www.google.com World http://yahoo.com  http:\\yahoo.com   http://www.yahoo.com')



    import Modules.Parsing.ner as ner

    nerc = ner.UrlRecognizer()

    #nerc.recognize(r'Hello www.google.com www.google.com World http://yahoo.com  http:\\yahoo.com   http://www.yahoo.com  ftp://www.yahoo.com')
    res = nerc.recognize(r'bostonclub.ru  #челябинск #вчелябинске #английскиекурсы #английскийиндивидуально #английскийрепетитор #языковойцентр #языковаяшкола \
                    #бостонклуб  Компания "Бостон Клуб" в социальных сетях:  ВКонтакте => https://vk.com/bostonclubchelyabinsk \
                    #Одноклассники => https://ok.ru/group/57649564811421 Фейсбук => https://www.facebook.com/bostonclubchelyabinsk/ \
                    #Фейсбук => https://www.facebook.com/groups/bostonclub/ Твиттер => https://twitter.com/bostonclub_ru \
                    #Мой Мир => https://my.mail.ru/community/bostonclubchelyabinsk/ Пинтерест => https://www.pinterest.ru/bostonclubchelyabinsk/ \
                    #Инстаграм => https://www.instagram.com/bostonclubchelyabinsk/ Телеграм => https://t.me/bostonclubchelyabinsk  \
                    #Бостон Клуб - Курсы английского языка для детей и взрослых в Челябинске.')
    a = 1
    #import Modules.Parsing.token as token
    #dp = token.SentenceTokenizer()
    #txt_lst = [ '  я видимо здоровья дохуя я работаю\n При поддержке Золота Бородача  ', '  я видимо здоровья дохуя я работаю\nПри поддержке Золота Бородача  \n' ]
    #res = dp.tokenize(txt_lst)


    #clear_tables_by_project(ID_PROJECT_main)
    #import sys
    #sys.exit(0)
    pass

if step_name == 'debug_sent_list':
    #debug_id_sent_list = [27678,27679,27680,27681,27682,27683,27684,27685,27686,27687,27688,27689,27690,27691,27692,27693,27694,27695,27696,27697,27698,27699,27700,27701,27702,27703,27704,27705,27706,27707,27708]
    debug_id_sent_list = [4637]

    dp = processor.NerProcessor(db = cass_db,
                    id_project = ID_PROJECT_main,
                    id_www_source = TG_SOURCE_ID,
                    need_stop_cheker = need_stop_cheker,
                    debug_mode = DEBUG_MODE,
                    msg_func = plpy.notice,
                    portion_size = 100)

    dp.debug_num_portions = 1

    dp.debug_sentence_id_list(debug_id_sent_list)



if step_name == 'process':
    clear_tables_by_project(ID_PROJECT_main)

    dp = processor.NerProcessor(db = cass_db,
                    id_project = ID_PROJECT_main,
                    id_www_source = TG_SOURCE_ID,
                    need_stop_cheker = need_stop_cheker,
                    debug_mode = DEBUG_MODE,
                    msg_func = plpy.notice,
                    portion_size = 100)

    dp.debug_num_portions = 10

    dp.process()
    pass


