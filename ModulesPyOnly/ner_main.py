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
    #import Modules.Parsing.ner as ner
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

    testString = r'Демонтаж  ----------------------------------------------------- РАЗГРУЗКА: ✓ ФУР ✓ВАГОНОВ ✓КОНТЕЙНЕРОВ ✓СКЛАДОВ ✓ И .Т .Д ПЕРЕВОЗКА: ХОЛОДИЛЬНИКОВ, СЕЙФОВ , ПИАНИН ,ДИВАНОВ , СТИРАЛЬНЫХ МАШИН , И.Т.Д -----------------------------------------------------  РАЗНОРАБОЧИЕ ✓ ЗЕМЛЯКОПЫ ✓ПОМОЩЬ ПО САДУ ✓ПОМОЩЬ ПО СТРОЙКИ ✓ УБОРКА СНЕГА С КРЫШ  -------------------------------------------------  ВЫЕЗД В ТЕЧЕНИЕ ЧАСА ------------------------------------------------- ГРУЗЧИКИ ГРАЖДАНЕ РФ ГАЗЕЛИ ЛЮБЫХ РАЗМЕРОВ КАМАЗЫ 10-15-25 ТОН ТЕХНИКА ДЖИСИБИ САМОПОГРУЗЧИКИ -------------------------------------------------  Любой объём работы Любой район города ВЫВОЗ СТРОИТЕЛЬНОГО МУСОРА'
    testList = []
    testList.append(testString)
    testList.append(testString)

    import Modules.Parsing.ner as ner
    nerc = ner.DoubleSimbolsRemover()

    nerc.remove_from_list(testList)

    a=1

    if False:
        import Modules.Parsing.ner as ner

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


    #nerc = ner.UrlRecognizer()

    ##nerc.recognize(r'Hello www.google.com www.google.com World http://yahoo.com  http:\\yahoo.com   http://www.yahoo.com  ftp://www.yahoo.com')
    #res = nerc.recognize(r'bostonclub.ru  #челябинск #вчелябинске #английскиекурсы #английскийиндивидуально #английскийрепетитор #языковойцентр #языковаяшкола \
    #                #бостонклуб  Компания "Бостон Клуб" в социальных сетях:  ВКонтакте => https://vk.com/bostonclubchelyabinsk \
    #                #Одноклассники => https://ok.ru/group/57649564811421 Фейсбук => https://www.facebook.com/bostonclubchelyabinsk/ \
    #                #Фейсбук => https://www.facebook.com/groups/bostonclub/ Твиттер => https://twitter.com/bostonclub_ru \
    #                #Мой Мир => https://my.mail.ru/community/bostonclubchelyabinsk/ Пинтерест => https://www.pinterest.ru/bostonclubchelyabinsk/ \
    #                #Инстаграм => https://www.instagram.com/bostonclubchelyabinsk/ Телеграм => https://t.me/bostonclubchelyabinsk  \
    #                #Бостон Клуб - Курсы английского языка для детей и взрослых в Челябинске.')
    #a = 1
    #import Modules.Parsing.token as token
    #dp = token.SentenceTokenizer()
    #txt_lst = [ '  я видимо здоровья дохуя я работаю\n При поддержке Золота Бородача  ', '  я видимо здоровья дохуя я работаю\nПри поддержке Золота Бородача  \n' ]
    #res = dp.tokenize(txt_lst)


    #clear_tables_by_project(ID_PROJECT_main)
    #import sys
    #sys.exit(0)
    pass

if step_name == 'debug_sent_list':
    #debug_id_sent_list = debug_id_sent_list = [101947,101948,101949,101950,101951,101952,101953,101954,101955,101956,101957,101990,102049,101959,101960,101961,101962,101963,101964,101965,101966,101967,101968,101969,101970,101971,101972,101973,101974,101975,101976,101977,101978,101979,101980,101981,101982,101983,101984,101985,101986,101987,101988,101989,101991,101992,101993,101994,101995,101996,101997,101998,101999,102000,102001,102002,102003,102004,102005,102007,102008,102009,102010,102011,102012,102013,102014,102015,102016,102017,102018,102019,102020,102021,102022,102023,102024,102025,102026,102027,102028,102029,102030,102031,102032,102033,102034,102035,102036,102037,102038,102039,102040,102041,102042,102043,102044,102045,102046,102050,102051,102052,102053,102054,102055,102056,102057,102058,102059,102060,102061,102062,102063,102064,102065,102066,102067,102068,102069,102070,102071,102072,102074,102075,102076,102077,102078,102079,102080,102081,102082,102083,102084,102085,102086,102087,102088,102089,102090,102091,102092,102093,102094,102095,102096,102097,102098,102099,102100,102101,102102,102103,102104,102105,102106,102107,102108,102109,102110,102111,102112,102113,102114,102115,102116,102117,102118,102119,102120,102121,102122,102123,102124,102125,102126,102127,102128,102129,102130,102131,102132,102133,102134,102135,102136,102137,102138,102139,102140,102141,102142,102143,102144,102145,102146,102147,102148,102149]
    debug_id_sent_list = [102107]
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
    #cass_db.custom_simple_request(f'UPDATE git400_token.sentence SET is_process = FALSE WHERE is_process = TRUE AND id_project = {ID_PROJECT_main} AND id_www_sources = {TG_SOURCE_ID}')
    #clear_tables_by_project(ID_PROJECT_main)

    dp = processor.NerProcessor(db = cass_db,
                    id_project = ID_PROJECT_main,
                    id_www_source = TG_SOURCE_ID,
                    need_stop_cheker = need_stop_cheker,
                    debug_mode = DEBUG_MODE,
                    msg_func = plpy.notice,
                    portion_size = 200)

    dp.debug_num_portions = 300

    dp.process()
    pass


