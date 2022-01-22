from datetime import time as timedt

import modules.common_mod.const as const
import modules.common_mod.pginterface as pginterface
import modules.common_mod.pauser as pauser
import modules.common_mod.jobs as jobs

import modules.crawling.exceptions as exceptions

import modules.parsing.processor as processor

####################################################
####### begin: for PY environment only #############
job_id = 201
job_id = None

step_name = 'process'
step_name = 'process_shedule'
step_name = 'debug'
step_name = 'debug_sent_list'

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
        cass_db.clear_table_by_project(t, id_project)


cass_db = pginterface.MainDB(plpy, GD)
need_stop_cheker = pginterface.NeedStopChecker(cass_db, ID_PROJECT_main, 'ner_recognize', state = 'off')



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

            #import modules.parsing.ner as ner
            #mld = ner.MixedLettersDetector()
            #testString = r'- пpeдлoжeниe дeйcтвyeт тoлькo в oтнoшeнии тex лиц, ктo гoтoв зaключить дoгoвop нaймa yкaзaннoгo жилoгo пoмeщeния и aктyaльнo в тeчeниe мecяцa, * дoм нoвый; * oгopoжeннaя дeтcкaя плoщaдкa; * ecть вceгдa пapкoвoчнoe мecтo для мaшины вo двope дoмa; * зa дoмoм ecть плaтнaя aвтocтoянкa; * paзвитa инфpacтpyктypa: - в шaгoвoй дocтyпнocти ocтaнoвкa oбщecтвeннoгo тpaнcпopтa 51-й микpopaйoн (20 мeтpoв oт дoмa); - в coceднeм дoмe дeтcкий caд "Чyнгa-чaнгa" (Лoбыpинa, д.7); - в 10 минyтax xoдьбы oт дoмa Oбpaзoвaтeльный цeнтp №2, MAOУ шкoлa, дeтcкий caд 19, Дeтcкий caд 45; - Пpямo в дoмe: Kpacнoe и бeлoe, пapикмaxepcкaя; - B 5 минyтax oт дoмa мaгaзины ceтeвыe : Пятёpoчкa , Maгнит и т.д.; - дo цeнтpa гopoдa 10-ть минyт нa мaшинe и 20-ть нa oбщecтвeннoм тpaнcпopтe; - мнoгo paзвивaющиx цeнтpoв кaк для дeтeй дoшкoльнoгo и шкoльнoгo вoзpacтa, тaк и для взpocлыx.'
            #res = mld.is_sentence_mixed(testString)
            #print('result '+str(res))
    
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
            debug_id_sent_list = [1810681,1810682,1810683,1810684,1810685,1810686,1810687,1810688,1810689,1810690,1810691,1810692,1810693,1810694,1810695,1810696,1810697,1810698,1810699,1810700,1810701,1810702,1810704,3155725,1810705,1810706,1810707,1810708,1810709,1810710,1810711,1810712,1810713,1810714,1810715,1810716,1810717,1810718,1810719,4516414,1810720,1810721,1810722,1810723,1810724,1810725,1810726,3155726,1810728,1810729,1810730,1810731,1810732,1810733,1810734,1810735,1810736,1810737,1810738,1810739,1810740,1810741,1810742,1810743,1810744,1810745,1810746,1810747,1810748,1810749,1810750,1810751,1810752,1810753,1810754,1810755,1810756,1810757,1810758,1810759,1810760,1810761,1810762,1810763,1810764,1810765,2374376,2374377,1810766,1810767,1810768,1810769,1810770,1810771,1810772,1810773,1810774,1810775,1810776,1810777]
            debug_id_sent_list = [4868317,4868572,4868573,4868574,4868708,4868856,4869014,4869015,4869016,4869063,4869064,4580529,4580530,4580558,4580559,4242264,4242265,4242266,4242267,4242268,4242269,4242270,4242271,4242272,4242273,4242274,4242275,4242276,4242277,4242279,4242280,4242281,4242282,4242283,4242284,4242285,4242286,4242287,4242288,4242289,4242290,4242291,4242292,4242293,4242294,4242295,4242296,4242297,4242298,4242299,4242300,4242302,4242303,4242304,4242305,4242306,4242307,4242308,4242309,4242310,4242311,4242312,4242313,4242314,4242315,4242316,4242317,4242318,4242319,4242320,4242321,4242322,4242323,4242324,4242325,4242326,4242327,4242328,4242329,4242330,4242331,4242332,4242333,4242334,4242335,4242336,4242337,4242338,4242339,4242340,4242341,4242342,4242343,4242344,4242345,4242346,4242347,4242348,4242349,4242350]
            debug_id_sent_list = [1810681,1810682,1810683,1810684,1810685,1810686,1810687,1810688,1810689,1810690,1810691,1810692,1810693,1810694,1810695,1810696,1810697,1810698,1810699,1810700,1810701,1810702,1810704,3155725,1810705,1810706,1810707,1810708,1810709,1810710,1810711,1810712,1810713,1810714,1810715,1810716,1810717,1810718,1810719,4516414,1810720,1810721,1810722,1810723,1810724,1810725,1810726,3155726,1810728,1810729,1810730,1810731,1810732,1810733,1810734,1810735,1810736,1810737,1810738,1810739,1810740,1810741,1810742,1810743,1810744,1810745,1810746,1810747,1810748,1810749,1810750,1810751,1810752,1810753,1810754,1810755,1810756,1810757,1810758,1810759,1810760,1810761,1810762,1810763,1810764,1810765,2374376,2374377,1810766,1810767,1810768,1810769,1810770,1810771,1810772,1810773,1810774,1810775,1810776,1810777]
            debug_id_sent_list = [1810728]
            ID_PROJECT_main = 10

            dp = processor.NerProcessor(db = cass_db,
                            id_project = ID_PROJECT_main,
                            id_www_source = gvars.get('TG_SOURCE_ID'),
                            need_stop_cheker = None,
                            debug_mode = DEBUG_MODE,
                            msg_func = plpy.notice,
                            portion_size = 200)

            dp.debug_num_portions = 1

            dp.debug_sentence_id_list(debug_id_sent_list)

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
    cass_db.log_fatal('CriticalErr on sentiment_main', ID_PROJECT_main, exceptions.get_err_description(e))
    raise










