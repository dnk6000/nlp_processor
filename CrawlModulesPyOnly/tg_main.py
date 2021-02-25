import datetime
import time
import json

import CrawlModulesPG.const as const
import CrawlModulesPG.date as date
import CrawlModulesPG.tg as tg
import CrawlModulesPG.pginterface as pginterface
import CrawlModulesPyOnly.plpyemul as plpyemul
import CrawlModulesPG.accounts as accounts
import CrawlModulesPG.crawler as crawler
import CrawlModulesPG.scraper as scraper
import CrawlModulesPG.pauser as pauser
import CrawlModulesPG.exceptions as exceptions

from CrawlModulesPG.globvars import GlobVars
if const.PY_ENVIRONMENT: 
    GD = None
else: 
    GD = {}
gvars = GlobVars(GD)


DEBUG_MODE = True
CHANNEL_SEARCH_KEYS = ['Челябинск','chelyabinsk','chelyab','челябин']

####### begin: for PY environment only #############
step_name = 'crawl_subscribers'
step_name = 'debug'
step_name = 'crawl_groups'
step_name = 'crawl_wall'
ID_PROJECT_main = 4

if const.PY_ENVIRONMENT:
    import CrawlModulesPyOnly.plpyemul as plpyemul
    import CrawlModulesPyOnly.self_psw as self_psw

    cassandra_db_conn_par = {
        'database': 'cassandra_new', 
        'host'   : '192.168.60.46', 
        'port': '5432', 
        'user': 'm.tyurin', 
        'password': self_psw.get_psw_db_mtyurin()
    }
    plpy = plpyemul.PlPy(**cassandra_db_conn_par)
#else:
#    try: 
#        plpy = None  #otherwise an error occurs: using the plpy before assignment
#    except:
#        pass

####### end: for PY environment only #############


def tg_crawl_groups(id_project, critical_error_counter = {'counter': 0}):

    need_stop_cheker = pginterface.NeedStopChecker(cass_db, id_project, 'crawl_group', state = 'off')

    request_error_pauser = pauser.ExpPauser()

    tg_crawler = tg.TelegramChannelsCrawler(debug_mode = DEBUG_MODE, 
                                            msg_func = plpy.notice, 
                                            need_stop_cheker = need_stop_cheker,
                                            search_keys = CHANNEL_SEARCH_KEYS, 
                                            request_error_pauser = request_error_pauser,
                                            **accounts.TG_ACCOUNT[0])
    
    tg_crawler.connect()

    select_result = cass_db.select_groups_id(id_project)
    
    tg_crawler.id_cash = list(i['account_id'] for i in select_result)

    CriticalErrorsLimit = 3

    for res_list in tg_crawler.crawling():
        _res_list = json.loads(res_list)
        n = len(_res_list)
        msg('Add groups to DB: ' + str(n))

        for res_unit in _res_list:

            msg(res_unit['result_type'])

            if res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_ACCOUNT:
                msg('Add account to DB: ' + str(res_unit['account_id']) + ' ' + res_unit['account_name'])
                cass_db.upsert_sn_accounts(TG_SOURCE_ID, id_project, const.SN_GROUP_MARK, **res_unit)
        
            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_ERROR:
                cass_db.log_error(res_unit['err_type'], id_project, res_unit['err_description'])
                msg(res_unit['err_type'])
                if res_unit['err_type'] in (const.ERROR_CONNECTION, const.ERROR_REQUEST_GET, const.ERROR_REQUEST_POST, const.ERROR_REQUEST_READ_TIMEOUT):
                    msg('Request error: pause before repeating...') #DEBUG
                    cass_db.log_info(const.LOG_INFO_REQUEST_PAUSE, id_project, request_error_pauser.get_description())
                    if not request_error_pauser.sleep():
                        raise exceptions.CrawlCriticalErrorsLimit(request_error_pauser.number_intervals)

            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_CRITICAL_ERROR:
                cass_db.log_fatal(res_unit['err_type'], id_project, res_unit['err_description'])
                wall_processed = False
                critical_error_counter['counter'] += 1

                if res_unit['stop_process']:
                    raise exceptions.StopProcess()

                if critical_error_counter['counter'] >= CriticalErrorsLimit:  
                    raise exceptions.CrawlCriticalErrorsLimit(critical_error_counter)


def tg_crawl_messages(id_project, id_group,  
                  project_params,
                  attempts_counter = 0, 
                  critical_error_counter = {'counter': 0},
                  queue = None,
                  debug_id_post = ''):
    
    #id_group = 'govoritfursov'  #DEBUG CROP ID

    if queue is not None:
        queue.reg_start()
        dt_start = queue.date_start_str
    else:
        dt_start = date.date_now_str()

    wall_processed = False
    CriticalErrorsLimit = 3

    need_stop_cheker = pginterface.NeedStopChecker(cass_db, id_project, 'crawl_wall', state = 'off')
    
    request_error_pauser = pauser.ExpPauser()

    sn_recrawler_checker = crawler.SnRecrawlerCheker(cass_db, 
                                                TG_SOURCE_ID, 
                                                id_project, 
                                                sn_id = id_group,#[0:12], #DEBUG CROP ID
                                                recrawl_days_post = project_params['recrawl_days_post'], 
                                                recrawl_days_reply = project_params['recrawl_days_reply'],
                                                plpy = plpy,
                                                tzinfo = datetime.timezone.utc)

    tg_crawler = tg.TelegramMessagesCrawler(debug_mode = DEBUG_MODE, 
                                            msg_func = plpy.notice, 
                                            id_group = id_group,
                                            debug_id_post = debug_id_post,
                                            need_stop_cheker = need_stop_cheker,
                                            sn_recrawler_checker = sn_recrawler_checker,
                                            date_deep = project_params['date_deep'],
                                            requests_delay_sec = project_params['requests_delay_sec'],
                                            request_error_pauser = request_error_pauser,
                                            **accounts.TG_ACCOUNT[0])
    msg(id_group)
    tg_crawler.connect()

    for res_list in tg_crawler.crawling(id_group):
        _res_list = json.loads(res_list)
        n = len(_res_list)
        c = 0
        msg('Add posts to DB: ' + str(n))

        for res_unit in _res_list:
            c += 1

            msg(res_unit['result_type'])
            
            if res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_NUM_SUBSCRIBERS:
                msg(res_unit['num_subscribers'])
                #cass_db.update_sn_num_subscribers(TG_SOURCE_ID, **res_unit)
            
            #elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_NUM_SUBSCRIBERS_NOT_FOUND:
            #    cass_db.log_error(res_unit['result_type'], id_project, res_unit['event_description'])
            
            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_DT_POST_ACTIVITY:
                msg('post id = {} dt = {}'.format(res_unit['sn_post_id'], res_unit['last_date']))
                cass_db.upsert_sn_activity(TG_SOURCE_ID, id_project, upd_date = dt_start, **res_unit) 

            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_FINISH_NOT_FOUND:
                cass_db.log_error(res_unit['result_type'], id_project, res_unit['event_description'])
                wall_processed = True
            
            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_FINISH_SUCCESS:
                cass_db.log_trace(res_unit['result_type'], id_project, res_unit['event_description'])
                wall_processed = True
            
            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_WARNING:
                cass_db.log_warn(res_unit['result_type'], id_project, res_unit['event_description'])
                if 'wall_processed' in res_unit:
                    wall_processed = res_unit['wall_processed']
            
            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_ERROR:
                cass_db.log_error(res_unit['err_type'], id_project, res_unit['err_description'])
                msg(res_unit['err_type'])
                if res_unit['err_type'] in (const.ERROR_CONNECTION, const.ERROR_REQUEST_GET, const.ERROR_REQUEST_POST, const.ERROR_REQUEST_READ_TIMEOUT):
                    msg('Request error: pause before repeating...') #DEBUG
                    cass_db.log_info(const.LOG_INFO_REQUEST_PAUSE, id_project, request_error_pauser.get_description())
                    if not request_error_pauser.sleep():
                        raise exceptions.CrawlCriticalErrorsLimit(request_error_pauser.number_intervals)

            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_CRITICAL_ERROR:
                cass_db.log_fatal(res_unit['err_type'], id_project, res_unit['err_description'])
                wall_processed = False
                critical_error_counter['counter'] += 1

                if queue is not None:  #this mechanism will be required when a problem is detected - one vk page is loaded, the other is not
                    queue.suspend(suspend_time_min=30)

                if res_unit['stop_process']:
                    raise exceptions.StopProcess()

                if critical_error_counter['counter'] >= CriticalErrorsLimit:  
                    raise exceptions.CrawlCriticalErrorsLimit(critical_error_counter)

            elif res_unit['result_type'] in (scraper.ScrapeResult.RESULT_TYPE_POST, scraper.ScrapeResult.RESULT_TYPE_REPLY, scraper.ScrapeResult.RESULT_TYPE_REPLY_TO_REPLY):
                msg('Add posts to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['sn_id']) + ' ' + res_unit['url'])
                cass_db.upsert_data_text(id_data_html = 0, id_project = id_project,  id_www_sources = TG_SOURCE_ID, **res_unit)
    
    if queue is not None:
        queue.reg_finish(wall_processed)
                                  

def tg_crawl_messages_start(id_project, queue):

    critical_error_counter = {'counter': 0}

    #project_params = cass_db.get_project_params(id_project)[0]          #DEBUG

    portion_counter = 0

    while True:
        project_params = cass_db.get_project_params(id_project)[0]  #temporarily in the loop to adjust the pause 

        if not queue.read_portion(portion_size = 1):
            break


        for elem in queue.portion_elements():
            tg_crawl_messages(id_project = id_project, 
                          id_group = elem['sn_id'], 
                          attempts_counter = elem['attempts_counter'], 
                          project_params = project_params,
                          critical_error_counter = critical_error_counter,
                          queue = queue)

def tg_crawl_messages_channel(id_project, id_group, id_post = ''):

    project_params = cass_db.get_project_params(id_project)[0]

    tg_crawl_messages(id_project = id_project, id_group = id_group, project_params = project_params, debug_id_post = id_post)

def msg(msgstr):
    if DEBUG_MODE:
        plpy.notice(msgstr)


cass_db = pginterface.MainDB(plpy, GD)
TG_SOURCE_ID = gvars.get('TG_SOURCE_ID')

cass_db.create_project(ID_PROJECT_main)



#--0-- debug
if step_name == 'debug':
    #clear_tables_by_project(ID_PROJECT_main)
    #cass_db.clear_table_by_project('git300_scrap.data_text', ID_PROJECT_main)
    #cass_db.clear_table_by_project('git200_crawl.sn_activity', ID_PROJECT_main)
    tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = 'govoritfurso', id_post = '480')
    pass

#--0-- clear
if step_name == 'clear_all':
    #for i in range(1,20):
    #    clear_tables_by_project(i)
    #clear_tables_by_project(ID_PROJECT_main)
    pass

#--1--
if step_name == 'crawl_groups':
    #cass_db.log_info('Start '+step_name, ID_PROJECT_main,'')
    tg_crawl_groups(ID_PROJECT_main)
    pass

#--2--
if step_name == 'crawl_subscribers':
    #cass_db.log_info('Start '+step_name, ID_PROJECT_main,'')
    #plpy.notice('GENERATE QUEUE id_project = {}'.format(ID_PROJECT_main));
    #cass_db.clear_table_by_project('git200_crawl.queue', ID_PROJECT_main)
    #cass_db.queue_generate(TG_SOURCE_ID, ID_PROJECT_main)
    #vk_crawl_wall_subscribers(ID_PROJECT_main)
    pass

#--3--
if step_name == 'crawl_wall':
    cass_db.log_info('Start '+step_name, ID_PROJECT_main,'')

    #cass_db.clear_table_by_project('git300_scrap.data_text', ID_PROJECT_main)
    #cass_db.clear_table_by_project('git200_crawl.sn_activity', ID_PROJECT_main)

    queue = crawler.QueueManager(id_source = TG_SOURCE_ID, id_project = ID_PROJECT_main, db = cass_db, min_subscribers=0)
    #queue.regenerate()
    tg_crawl_messages_start(ID_PROJECT_main, queue)



pass


