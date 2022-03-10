from datetime import time as timedt

import modules.common_mod.const as const
from modules.db.cassandra import Cassandra, NeedStopChecker
import modules.common_mod.pauser as pauser
import modules.common_mod.jobs as jobs

import modules.common_mod.exceptions as exceptions

import modules.parsing.processor as processor

####################################################
####### begin: for PY environment only #############
job_id = 201
job_id = None

step_name = 'process'
step_name = 'process_shedule'
step_name = 'debug_sent_list'
step_name = 'debug'

#will be apply for debug only: when job_id is None
ID_PROJECT_main = 10
SOURCE_ID = 4
DEBUG_MODE = True
PORTION_SIZE = 200
DEBUG_NUM_PORTIONS = 300

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
        'git430_ner.entity',
        'git400_token.word'
        ]
    for t in tables:
        plpy.notice('Delete table {} by project {}'.format(t,id_project))
        cass_db.query.clear_table_by_project(t, id_project)


cass_db = Cassandra(plpy, GD)
need_stop_cheker = NeedStopChecker(cass_db, ID_PROJECT_main, 'ner_recognize', state = 'off')



try:

    cass_db = Cassandra(plpy, GD)

    job = jobs.JobManager(id_job = job_id, db = cass_db)

    need_stop_cheker = NeedStopChecker.get_need_stop_cheker(job, cass_db, ID_PROJECT_main, 'tokenize')

    while job.get_next_step():

        if not job_id is None:
            step_params = job.get_step_params()
            step_name = step_params['step_name']
            ID_PROJECT_main = step_params['id_project']
            SOURCE_ID = step_params['id_source']
            PORTION_SIZE = step_params['portion_size']
            DEBUG_NUM_PORTIONS = step_params['debug_num_portions']
            DEBUG_MODE = step_params['debug_mode']

        cass_db.git000_cfg.create_project(ID_PROJECT_main)

        print(f'step_name: {step_name} ID_PROJECT: {ID_PROJECT_main}    SOURCE_ID: {SOURCE_ID}    PORTION_SIZE: {PORTION_SIZE}     DEBUG_NUM_PORTIONS: {DEBUG_NUM_PORTIONS}')


        if step_name == 'debug':
            TG_SOURCE_ID = gvars.get('TG_SOURCE_ID')
            VK_SOURCE_ID = gvars.get('VK_SOURCE_ID')

            if True:
                import modules.parsing.ner as ner
                mld = ner.MixedLettersDetector()
                testString = r'PAДЫ ПРИВЕТСТВОВАТЬ BCEX KЛИEНTOB ДЯДИ НАPKOЛOГA   Розничная сеть NARKOLOG_ONLINE - магазин моментальных кладов в вашем городе 24/7'
                res = mld.is_sentence_mixed(testString)
                print('result '+str(res))
    
            #testString = r'- предложение Yдействует Yтолько в отношении тех лиц, кто готов заключить договор найма'
            #res = mld.is_sentence_mixed(testString)
            #print('result '+str(res))



            #import re
            #r_words = re.compile("[а-яА-Яa-zA-Z]{2,}",re.I)
            #r_mixed_words = re.compile("([а-яА-Я]{1,}[a-zA-Z]{1,}|[a-zA-Z]{1,}[а-яА-Я]{1,})",re.I)

            #testString = r'- пpeдлoжeниe дeйcтвyeт тoлькo в oтнoшeнии тex лиц, ктo гoтoв зaключить дoгoвop нaймa yкaзaннoгo жилoгo пoмeщeния и aктyaльнo в тeчeниe мecяцa, * дoм нoвый; * oгopoжeннaя дeтcкaя плoщaдкa; * ecть вceгдa пapкoвoчнoe мecтo для мaшины вo двope дoмa; * зa дoмoм ecть плaтнaя aвтocтoянкa; * paзвитa инфpacтpyктypa: - в шaгoвoй дocтyпнocти ocтaнoвкa oбщecтвeннoгo тpaнcпopтa 51-й микpopaйoн (20 мeтpoв oт дoмa); - в coceднeм дoмe дeтcкий caд "Чyнгa-чaнгa" (Лoбыpинa, д.7); - в 10 минyтax xoдьбы oт дoмa Oбpaзoвaтeльный цeнтp №2, MAOУ шкoлa, дeтcкий caд 19, Дeтcкий caд 45; - Пpямo в дoмe: Kpacнoe и бeлoe, пapикмaxepcкaя; - B 5 минyтax oт дoмa мaгaзины ceтeвыe : Пятёpoчкa , Maгнит и т.д.; - дo цeнтpa гopoдa 10-ть минyт нa мaшинe и 20-ть нa oбщecтвeннoм тpaнcпopтe; - мнoгo paзвивaющиx цeнтpoв кaк для дeтeй дoшкoльнoгo и шкoльнoгo вoзpacтa, тaк и для взpocлыx.'
            #tokens = testString.split(' ')
            #words = [w for w in filter(r_words.match, tokens)]
            #mixed_words = [w for w in filter(r_mixed_words.match, words)]
            ##matchArray = regex.match(testString)
            ### the matchArray variable contains the list of matches
            ##re.finditer(r'(?:(?:https?|ftp|file):(//|\\)|www\.|ftp\.)', r'Hello www.google.com World http://yahoo.com  http:\\yahoo.com   http://www.yahoo.com')
            #print('len words '+str(len(words)))
            #print('len mixed words '+str(len(mixed_words)))
            #f=1
            #pass

            #testString = r'Демонтаж  ----------------------------------------------------- РАЗГРУЗКА: ✓ ФУР ✓ВАГОНОВ ✓КОНТЕЙНЕРОВ ✓СКЛАДОВ ✓ И .Т .Д ПЕРЕВОЗКА: ХОЛОДИЛЬНИКОВ, СЕЙФОВ , ПИАНИН ,ДИВАНОВ , СТИРАЛЬНЫХ МАШИН , И.Т.Д -----------------------------------------------------  РАЗНОРАБОЧИЕ ✓ ЗЕМЛЯКОПЫ ✓ПОМОЩЬ ПО САДУ ✓ПОМОЩЬ ПО СТРОЙКИ ✓ УБОРКА СНЕГА С КРЫШ  -------------------------------------------------  ВЫЕЗД В ТЕЧЕНИЕ ЧАСА ------------------------------------------------- ГРУЗЧИКИ ГРАЖДАНЕ РФ ГАЗЕЛИ ЛЮБЫХ РАЗМЕРОВ КАМАЗЫ 10-15-25 ТОН ТЕХНИКА ДЖИСИБИ САМОПОГРУЗЧИКИ -------------------------------------------------  Любой объём работы Любой район города ВЫВОЗ СТРОИТЕЛЬНОГО МУСОРА'
            #testList = []
            #testList.append(testString)
            #testList.append(testString)

            #import modules.parsing.ner as ner
            #nerc = ner.DoubleSimbolsRemover()

            #nerc.remove_from_list(testList)

            #a=1

            if False:
                import modules.parsing.ner as ner

                nerc = ner.NerConsolidator()

                #orig_nt = [ 'O'     , 'B-PER','I-PER'  , 'GIT-URL'   , 'B-PER' , 'O'     , 'GIT-URL'   , 'O', 'I-LOC'  , 'I-LOC', 'O', 'O'        , 'O', 'O'     , 'B-LOC'    , 'GIT-URL'   , 'GIT-URL'   , 'I-LOC'  , 'B-LOC' ]
                #orig_n  = [ 'Солдат', 'Иван' ,'Петров' , 'www.ria.ru', 'Петров', 'пришел', 'www.git.ru', 'в', 'Красное', 'Село' , 'и', 'поселился', 'в', 'районе', 'Дворцовый', 'www.git.ru', 'www.ria.ru', 'Село', 'Красное' ]
                orig_nt = ['B-PER', 'O', 'O', 'O', 'O', 'GIT-URL','B-PER', 'B-PER', 'O']
                orig_n  = ['ленин', '81', 'bostonclub', '.', 'ru', 'http://bostonclub.ru', 'Иван', 'Петров', '.']
                nt_res, n_res, idx_res = nerc.consolidate(orig_nt, orig_n)

                print(n_res)
                print(nt_res)
                print(idx_res)

                n = 0
                for i in zip(nt_res, n_res):
                    print(f'{n:>4}  {i[1]:>15}          {i[0]:>9}')
                    n += 1

                print('----------------')
                for i in zip(orig_nt, orig_n, idx_res):
                    t = i[2] if i[2] is not None else ''
                    print(f'{i[1]:>15}          {i[0]:>9}          {t:>5}')

            pass

            if False:
                import modules.parsing.ner as ner
                nerc = ner.UrlRecognizer()

                ##nerc.recognize(r'Hello www.google.com www.google.com World http://yahoo.com  http:\\yahoo.com   http://www.yahoo.com  ftp://www.yahoo.com')
            
                #res = nerc.recognize(r'bostonclub.ru  #челябинск #вчелябинске #английскиекурсы #английскийиндивидуально #английскийрепетитор #языковойцентр #языковаяшкола \
                #                #бостонклуб  Компания "Бостон Клуб" в социальных сетях:  ВКонтакте => https://vk.com/bostonclubchelyabinsk \
                #                #Одноклассники => https://ok.ru/group/57649564811421 Фейсбук => https://www.facebook.com/bostonclubchelyabinsk/ \
                #                #Фейсбук => https://www.facebook.com/groups/bostonclub/ Твиттер => https://twitter.com/bostonclub_ru \
                #                #Мой Мир => https://my.mail.ru/community/bostonclubchelyabinsk/ Пинтерест => https://www.pinterest.ru/bostonclubchelyabinsk/ \
                #                #Инстаграм => https://www.instagram.com/bostonclubchelyabinsk/ Телеграм => https://t.me/bostonclubchelyabinsk  \
                #                #Бостон Клуб - Курсы английского языка для детей и взрослых в Челябинске.')

                tstlst = [r'https://vk.com/id151183292), Ирина Κононова (https://vk.com/id395075905), Эльвира Асылхужина (https://vk.com/id377690170), \
                                        Катерина Должина (https://vk.com/id202686765), Ксения Селютина (https://vk.com/id72663942), Елена Водолазова (https://vk.com/id11917119), \
                                        Нина Арбузина (https://vk.com/id204323150), Анастасия Пястолова (https://vk.com/id132626821), Анна Луч (https://vk.com/id90594528), \
                                        Екатерина Якушева (https://vk.com/id485916886), Юлия Алексеева (https://vk.com/id111931127), Екатерина Марьина (https://vk.com/id496687789), \
                                        Диана Быкова (https://vk.com/id358449811), Наташа Михеенкова (https://vk.com/id255235153), Юлиана Муханова (https://vk.com/id155088539), \
                                        Оля Маленькая вредина (https://vk.com/id135052683), Евгения Заварухина (https://vk.com/id134445826), Кристина Гренц (https://vk.com/id110775169), \
                                        Марина Какушина (https://vk.com/id107662650), Катерина Гордеева (https://vk.com/id7793912), Tatyana Nails (https://vk.com/id492155573), \
                                        Елена Ноготкова (https://vk.com/id482112910), Nastya Murashova (https://vk.com/id460967460), Anna Krasnova (https://vk.com/id442065690), \
                                        Валентина Милехина (https://vk.com/id424672278), Юлия Ненадовец (https://vk.com/id249657264), Алёна Кузнецова (https://vk.com/id15869847), \
                                        Ксения Калашникова (https://vk.com/id176490828), Регина Сомова (https://vk.com/id507304555), Наталья Натальевна (https://vk.com/id287518468), \
                                        Светлана Плеханова (https://vk.com/id64542912), Милена Вениаминовна (https://vk.com/id336402536), Юлия Барисевич (https://vk.com/id21693083), \
                                        Мидина Маймакова (https://vk.com/id422892795), Оксана Петрова (https://vk.com/id508712311), Анастасия Галкина (https://vk.com/id430093901), \
                                        Людочка Калинова (https://vk.com/id469554172), Диана Минькина (https://vk.com/id373761695), Аня Лисовская (https://vk.com/id410138720), \
                                        Оксана Кофман (https://vk.com/id548032773), Tatyana Brows (https://vk.com/id636568309), Ирина Шумакова (https://vk.com/id619269276), \
                                        Tanya Silantyeva (https://vk.com/id590753993), Милана Синицына (https://vk.com/id577705685), Вера Переверзева (https://vk.com/id576121462), \
                                        Ольга Гришина (https://vk.com/id572001478), Conor Leslie (https://vk.com/id562712381), Виктория Массажная (https://vk.com/id536686098), \
                                        Алёна Кузнецова (https://vk.com/id525490068)  С Днем Рождения!']

                res = nerc.recognize(tstlst)
            pass
            
            #a = 1
            #import modules.parsing.token as token
            #dp = token.SentenceTokenizer()
            #txt_lst = [ '  я видимо здоровья дохуя я работаю\n При поддержке Золота Бородача  ', '  я видимо здоровья дохуя я работаю\nПри поддержке Золота Бородача  \n' ]
            #res = dp.tokenize(txt_lst)


            #clear_tables_by_project(ID_PROJECT_main)
            #import sys
            #sys.exit(0)
            pass

        if step_name == 'debug_sent_list':
            debug_id_sent_list = [4243299,4243300,4243301,4243302,4243303,4243304,4243305,4243306,4243307,4243308,4243309,4243310,4243311,4243312,4243313,4243314,4243315,4581873,4243316,4243317,4243318,4243319,4243320,4243321,4243322,4243323,4243324,4243325,4243326,4243327,4243328,4243329,4243330,4243331,4243332,4243333,4243334,4243335,4243336,4243337,4243338,4243339,4243342,4243343,4243344,4243345,4243346,4243347,4243348,4243349,4243350,4243351,4243352,4243353,4243354,4243355,4243356,4243357,4243358,4243359,4243360,4243361,4243362,4243363,4243364,4243365,4243366,4243367,4243368,4243369,4243370,4243371,4243372,4582037,4582065,4582066,4582093,4582094,4243373,4243374,4243375,4243376,4243378,4243379,4243380,4243381,4243382,4243383,4243384,4243385,4243386,4243387,4243388,4243389,4582095,4243390,4243391]            
            #debug_id_sent_list = [1810728]
            #debug_id_sent_list = [1810681,1810728,1810683]
            ID_PROJECT_main = 10

            dp = processor.NerProcessor(db = cass_db,
                            id_project = ID_PROJECT_main,
                            id_www_source = gvars.get('TG_SOURCE_ID'),
                            need_stop_cheker = None,
                            debug_mode = DEBUG_MODE,
                            msg_func = plpy.notice,
                            portion_size = 200)

            dp.debug_num_portions = 1

            #dp.debug_sentence_id_list_one_by_one(debug_id_sent_list)
            dp.debug_sentence_id_list_whole(debug_id_sent_list)

        if step_name == 'process':
            #cass_db.custom_simple_request(f'UPDATE git400_token.sentence SET is_process = FALSE WHERE is_process = TRUE AND id_project = {ID_PROJECT_main} AND id_www_sources = {TG_SOURCE_ID}')
            #clear_tables_by_project(ID_PROJECT_main)

            dp = processor.NerProcessor(db = cass_db,
                            id_project = ID_PROJECT_main,
                            id_www_source = SOURCE_ID,
                            need_stop_cheker = need_stop_cheker,
                            debug_mode = DEBUG_MODE,
                            msg_func = plpy.notice,
                            portion_size = PORTION_SIZE)

            dp.debug_num_portions = DEBUG_NUM_PORTIONS

            dp.process()
            del dp #to free memory
            pass

        if step_name == 'process_shedule':

            #VK_ID_PROJECT = 9
            #TG_ID_PROJECT = 10

            #PORTION_SIZE = 500
            #NUM_PORTIONS = 50

            #shedule_pauser = pauser.DayShedulePauser()
            ##shedule_pauser.add_pause(timedt(9,0,0),timedt(23,59,59))

            #dp_vk = processor.NerProcessor(db = cass_db,
            #                id_project = VK_ID_PROJECT,
            #                id_www_source = VK_SOURCE_ID,
            #                need_stop_cheker = need_stop_cheker,
            #                debug_mode = DEBUG_MODE,
            #                msg_func = plpy.notice,
            #                portion_size = PORTION_SIZE)

            #dp_vk.debug_num_portions = NUM_PORTIONS

            #dp_tg = processor.NerProcessor(db = cass_db,
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
    cass_db.git999_log.log_fatal('CriticalErr on sentiment_main', ID_PROJECT_main, description=exceptions.get_err_description(e))
    raise










