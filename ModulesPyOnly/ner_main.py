import Modules.Common.const as const
import Modules.Common.pginterface as pginterface
import Modules.Parsing.processor as processor


####################################################
####### begin: for PY environment only #############
step_name = 'process'
step_name = 'debug_sent_list'
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
    import re
    r_words = re.compile("[а-яА-Яa-zA-Z]{2,}",re.I)
    r_mixed_words = re.compile("([а-яА-Я]{1,}[a-zA-Z]{1,}|[a-zA-Z]{1,}[а-яА-Я]{1,})",re.I)

    testString = r'- пpeдлoжeниe дeйcтвyeт тoлькo в oтнoшeнии тex лиц, ктo гoтoв зaключить дoгoвop нaймa yкaзaннoгo жилoгo пoмeщeния и aктyaльнo в тeчeниe мecяцa, * дoм нoвый; * oгopoжeннaя дeтcкaя плoщaдкa; * ecть вceгдa пapкoвoчнoe мecтo для мaшины вo двope дoмa; * зa дoмoм ecть плaтнaя aвтocтoянкa; * paзвитa инфpacтpyктypa: - в шaгoвoй дocтyпнocти ocтaнoвкa oбщecтвeннoгo тpaнcпopтa 51-й микpopaйoн (20 мeтpoв oт дoмa); - в coceднeм дoмe дeтcкий caд "Чyнгa-чaнгa" (Лoбыpинa, д.7); - в 10 минyтax xoдьбы oт дoмa Oбpaзoвaтeльный цeнтp №2, MAOУ шкoлa, дeтcкий caд 19, Дeтcкий caд 45; - Пpямo в дoмe: Kpacнoe и бeлoe, пapикмaxepcкaя; - B 5 минyтax oт дoмa мaгaзины ceтeвыe : Пятёpoчкa , Maгнит и т.д.; - дo цeнтpa гopoдa 10-ть минyт нa мaшинe и 20-ть нa oбщecтвeннoм тpaнcпopтe; - мнoгo paзвивaющиx цeнтpoв кaк для дeтeй дoшкoльнoгo и шкoльнoгo вoзpacтa, тaк и для взpocлыx.'
    tokens = testString.split(' ')
    words = [w for w in filter(r_words.match, tokens)]
    mixed_words = [w for w in filter(r_mixed_words.match, words)]
    #matchArray = regex.match(testString)
    ## the matchArray variable contains the list of matches
    #re.finditer(r'(?:(?:https?|ftp|file):(//|\\)|www\.|ftp\.)', r'Hello www.google.com World http://yahoo.com  http:\\yahoo.com   http://www.yahoo.com')
    print('len words '+str(len(words)))
    print('len mixed words '+str(len(mixed_words)))
    f=1
    pass


    #import Modules.Parsing.ner as ner

    #nerc = ner.NerConsolidator()

    #nt_res, n_res, idx_res = nerc.consolidate([ 'O'     , 'B-PER', 'B-PER' , 'O'     , 'O', 'I-LOC'  , 'I-LOC', 'O', 'O'        , 'O', 'O'     , 'B-LOC'     ],
    #                                 [ 'Солдат', 'Иван' , 'Петров', 'пришел', 'в', 'Красное', 'Село' , 'и', 'поселился', 'в', 'районе', 'Дворцовый' ]
    #                                )
    #print(n_res)
    #print(nt_res)
    #print(idx_res)
    #pass


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
    #debug_id_sent_list = [27678,27679,27680,27681,27682,27683,27684,27685,27686,27687,27688,27689,27690,27691,27692,27693,27694,27695,27696,27697,27698,27699,27700,27701,27702,27703,27704,27705,27706,27707,27708]
    #debug_id_sent_list = [4637]
    #debug_id_sent_list = [33793,33794,33795,33796,37496,34477,33797,34478,34479,43526,43527,44134,34480,44333,44590,22888,34481,44464,44824,44527,34519,34520,33807,33808,33809,33810,34521,45069,45083,34522,33811,12010,12011,45349,45388,12012,34482,34483,12013,45585,45619,12014,45639,12015,12016,45716,12017,34484,33834,24415,33840,33841,24416,46132,33842,33843,34485,33844,12766,12767,33845,37703,33846,33848,46721,47335,37704,33868,11448,11449,11450,11451,11452,11453,11454,11455,11456,11457,13669,33318,33319,33322,33323,33324,33325,34523,33326,33327,33328,33329,33330]
    #debug_id_sent_list = [88150,88151,88152,88153,88154,88155,88156,88157,88158,88159,88160,88161,88162,88163,88186,88187,88188,88189,88190,88191,88192,88193,88194,88195,88196,88197,88198,88199,88200,88201,88202,88203,88204,88205,88206,88207,88208,88209,88210,88211,88212,88213,88214,88215,88216,88217,88218,88222,88223,88224,88225,88226,88227,88228,88229,88230,88231,88232,88233,88234,88235,88236,88237,88238,88239,88240,88241,88242,88243,88244,88245,88246,88247,88248,88249,88250,88251,88252,88253,88353,89527,88265,88266,88267,88268,88269,88270,88271,88272,88273,88274,88275,88276,88277,88278,88279,88280,88281,88282,88283,88284,88285,88286,88287,88288,88289,88290,88291,88292,88293,88294,88295,88296,88297,88298,88299,88300,88354,88301,88302,88303,88304,88305,88306,88307,88308,88309,88310,88311,88312,88313,88314,88315,88316,88317,88318,88319,88320,88321,88322,88323,88324,88325,88326,88327,88328,88329,88330,88331,88332,88355,88356,89637,88333,88334,88335,88336,88337,88338,88339,88340,88341,88342,88343,88344,88345,88346,88347,88348,88349,88350,88351,88352,88357,88358,88359,88360,88361,88362,88363,88364,88365,88366,88367,88368,88369,88370,88371,88372,88373,88374,88375,88376,88377,88378,88379,88380,88381,88382,88383]
    debug_id_sent_list = [88230]
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
    cass_db.custom_simple_request(f'UPDATE git400_token.sentence SET is_process = FALSE WHERE is_process = TRUE AND id_project = {ID_PROJECT_main} AND id_www_sources = {TG_SOURCE_ID}')
    clear_tables_by_project(ID_PROJECT_main)

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


