import datetime
import time
import json

import modules.common_mod.venv as venv
import modules.common_mod.const as const
import modules.common_mod.common as common
import modules.common_mod.pginterface as pginterface
import modules.common_mod.proxy as proxy
import modules.common_mod.pauser as pauser
import modules.common_mod.jobs as jobs

import modules.crawling.date as date
import modules.crawling.tg as tg
import modules.crawling.accounts as accounts
import modules.crawling.crawler as crawler
import modules.crawling.scraper as scraper
import modules.crawling.exceptions as exceptions

from modules.common_mod.globvars import GlobVars
if const.PY_ENVIRONMENT: 
    GD = None
else: 
    GD = {}
gvars = GlobVars(GD)


DEBUG_MODE = True
#DEBUG_MODE = False

####################################################
####### begin: for PY environment only #############
job_id = 1
job_id = None

step_name = 'crawl_wall'
step_name = 'crawl_groups'
step_name = 'debug'
ID_PROJECT_main = 1
queue_generate = True

if const.PY_ENVIRONMENT:
    import ModulesPyOnly.plpyemul as plpyemul
    plpy = plpyemul.get_plpy()
else:
    #    try: 
    #        plpy = None  #otherwise an error occurs: using the plpy before assignment
    #    except:
    #        pass
    pass

####### end: for PY environment only #############
####################################################

def tg_crawl_groups(id_project, job = None, critical_error_counter = {'counter': 0}, update_hash = False):

    project_params = cass_db.get_project_params(id_project)[0] 
    group_search_str = project_params['group_search_str']

    if group_search_str.isspace():
        cass_db.log_error(const.CW_RESULT_TYPE_ERROR, id_project, 'Search string is empty!')
        return

    base_search_words = group_search_str.split(',')

    need_stop_cheker = pginterface.NeedStopChecker.get_need_stop_cheker(job, cass_db, id_project, 'crawl_group')

    request_error_pauser = pauser.ExpPauser()

    project_proxy = proxy.ProxyCassandra(cass_db = cass_db, id_project = id_project, msg_func = plpy.notice)

    tg_crawler = tg.TelegramChannelsCrawler(debug_mode = DEBUG_MODE, 
                                            msg_func = msg, #plpy.notice, 
                                            need_stop_cheker = need_stop_cheker,
                                            search_keys = base_search_words, 
                                            requests_delay_sec = project_params['requests_delay_sec'],
                                            request_error_pauser = request_error_pauser,
                                            proxy = project_proxy,
                                            **accounts.TG_ACCOUNT[0])
    
    tg_crawler.connect()

    if not update_hash:
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
                        tg_crawler.close_session()
                        raise exceptions.CrawlCriticalErrorsLimit(request_error_pauser.number_intervals)

            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_CRITICAL_ERROR:
                cass_db.log_fatal(res_unit['err_type'], id_project, res_unit['err_description'])
                wall_processed = False
                critical_error_counter['counter'] += 1

                if res_unit['stop_process']:
                    tg_crawler.close_session()
                    raise exceptions.StopProcess()

                if critical_error_counter['counter'] >= CriticalErrorsLimit:  
                    tg_crawler.close_session()
                    raise exceptions.CrawlCriticalErrorsLimit(critical_error_counter)

    tg_crawler.close_session()

def tg_crawl_messages(id_project, id_group, name_group, hash_group,
                  project_params,
                  job = None,
                  attempts_counter = 0, 
                  critical_error_counter = {'counter': 0},
                  queue = None,
                  debug_id_post = '',
                  tg_client = { 'client': None }):
    
    #id_group = 'govoritfursov'  #DEBUG CROP ID

    if queue is not None:
        queue.reg_start()
        dt_start = queue.date_start_str
    else:
        dt_start = date.date_now_str()

    wall_processed = False
    CriticalErrorsLimit = 3

    need_stop_cheker = pginterface.NeedStopChecker.get_need_stop_cheker(job, cass_db, id_project, 'crawl_wall')
    
    request_error_pauser = pauser.ExpPauser()

    project_proxy = proxy.ProxyCassandra(cass_db = cass_db, id_project = id_project, msg_func = plpy.notice)

    sn_recrawler_checker = crawler.SnRecrawlerCheker(cass_db, 
                                                TG_SOURCE_ID, 
                                                id_project, 
                                                sn_id = id_group,
                                                recrawl_days_post = project_params['recrawl_days_post'], 
                                                recrawl_days_reply = project_params['recrawl_days_reply'],
                                                plpy = plpy,
                                                msg_func = msg, #plpy.notice,
                                                tzinfo = datetime.timezone.utc,
                                                utc_hours_delta = const.HOURS_TZ_UTC_OFFSET)

    tg_crawler = tg.TelegramMessagesCrawler(debug_mode = DEBUG_MODE, 
                                            msg_func = msg, #plpy.notice, 
                                            id_group = id_group,
                                            name_group = name_group,
                                            hash_group = hash_group,
                                            debug_id_post = debug_id_post,
                                            need_stop_cheker = need_stop_cheker,
                                            sn_recrawler_checker = sn_recrawler_checker,
                                            date_deep = project_params['date_deep'],
                                            requests_delay_sec = project_params['requests_delay_sec'],
                                            request_error_pauser = request_error_pauser,
                                            proxy = project_proxy,
                                            **accounts.TG_ACCOUNT[0])
    msg(id_group)
    if tg_client['client'] is None:
        tg_crawler.connect()
        tg_client['client'] = tg_crawler.client
    else:
        tg_crawler.client = tg_client['client']

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
                cass_db.set_sn_activity_fin_date(TG_SOURCE_ID, id_project, id_group, date.date_now_str())
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
                    msg(f'{date.date_now_str()}: Request error: pause before repeating...') #DEBUG
                    cass_db.log_info(const.LOG_INFO_REQUEST_PAUSE, id_project, request_error_pauser.get_description())
                    if not request_error_pauser.sleep():
                        tg_crawler.close_session()
                        raise exceptions.CrawlCriticalErrorsLimit(request_error_pauser.number_intervals)

            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_CRITICAL_ERROR:
                cass_db.log_fatal(res_unit['err_type'], id_project, res_unit['err_description'])
                wall_processed = False
                critical_error_counter['counter'] += 1

                if queue is not None:  #this mechanism will be required when a problem is detected - one vk page is loaded, the other is not
                    queue.suspend(suspend_time_min=30)

                if res_unit['stop_process']:
                    tg_crawler.close_session()
                    raise exceptions.StopProcess()

                if critical_error_counter['counter'] >= CriticalErrorsLimit:  
                    tg_crawler.close_session()
                    raise exceptions.CrawlCriticalErrorsLimit(critical_error_counter)

            elif res_unit['result_type'] in (scraper.ScrapeResult.RESULT_TYPE_POST, scraper.ScrapeResult.RESULT_TYPE_REPLY, scraper.ScrapeResult.RESULT_TYPE_REPLY_TO_REPLY):
                msg('Add posts to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['sn_id']) + ' ' + res_unit['url'])
                cass_db.upsert_data_text(id_data_html = 0, id_project = id_project,  id_www_sources = TG_SOURCE_ID, **res_unit)
    
    tg_crawler.close_session()

    if queue is not None:
        queue.reg_finish(wall_processed)
                                  

def tg_crawl_messages_start(id_project, queue, job = None):

    critical_error_counter = {'counter': 0}

    #project_params = cass_db.get_project_params(id_project)[0]          #DEBUG

    portion_counter = 0
    tg_client = { 'client': None }

    while True:
        project_params = cass_db.get_project_params(id_project)[0]  #temporarily in the loop to adjust the pause 

        if not queue.read_portion(portion_size = 1):
            break


        for elem in queue.portion_elements():
            tg_crawl_messages(id_project = id_project, 
                          id_group = elem['sn_id'], 
                          name_group = elem['account_name'], 
                          hash_group = elem['account_extra_1'], 
                          parameters = elem['parameters'],
                          attempts_counter = elem['attempts_counter'], 
                          project_params = project_params,
                          critical_error_counter = critical_error_counter,
                          queue = queue,
                          tg_client = tg_client,
                          job = job)

def tg_crawl_messages_channel(id_project, id_group, name_group, hash_group = '', parameters = '', id_post = '', job = None):

    project_params = cass_db.get_project_params(id_project)[0]

    tg_crawl_messages(id_project = id_project, 
                      id_group = id_group, 
                      name_group = name_group, 
                      hash_group = hash_group, 
                      parameters = parameters,
                      project_params = project_params, 
                      debug_id_post = id_post,
                      job = job)
    
    pass

def tg_add_group(id_project, name_group):

    project_proxy = proxy.ProxyCassandra(cass_db = cass_db, id_project = id_project, msg_func = plpy.notice)

    tg_crawler = tg.TelegramChannelsCrawler(debug_mode = DEBUG_MODE, 
                                            msg_func = msg, #plpy.notice, 
                                            proxy = project_proxy,
                                            **accounts.TG_ACCOUNT[0])
    
    tg_crawler.connect()
    res = tg_crawler.direct_add_group(name_group)
    res_list = json.loads(res)
    res_unit = res_list[0]
    msg('Add account to DB: ' + str(res_unit['account_id']) + ' ' + res_unit['account_name'])
    cass_db.upsert_sn_accounts(TG_SOURCE_ID, id_project, const.SN_GROUP_MARK, **res_unit)

    tg_crawler.close_session()

    pass

def msg(msgstr):
    if DEBUG_MODE:
        if logger is not None:
            logger.info(msgstr)
        plpy.notice(msgstr)

def clear_tables_by_project(id_project):
    tables = [
        'git300_scrap.data_text',
        'git200_crawl.data_html',
        'git200_crawl.queue',
        'git200_crawl.sn_activity',
        'git200_crawl.sn_accounts',
        'git999_log.log'
        ]
    for t in tables:
        plpy.notice('Delete table {} by project {}'.format(t,id_project))
        cass_db.clear_table_by_project(t, id_project)

logger = None

try:

    cass_db = pginterface.MainDB(plpy, GD)

    TG_SOURCE_ID = gvars.get('TG_SOURCE_ID')

    job = jobs.JobManager(id_job = job_id, db = cass_db)

    while job.get_next_step():

        if not job_id is None:
            step_params = job.get_step_params()
            step_name = step_params['step_name']
            ID_PROJECT_main = step_params['id_project']
            DEBUG_MODE = step_params['debug_mode']
            if 'queue_generate' in step_params:
                queue_generate = step_params['queue_generate']
            else:
                queue_generate = True

        cass_db.create_project(ID_PROJECT_main)

        print(f'step_name: {step_name} ID_PROJECT: {ID_PROJECT_main}')

        if DEBUG_MODE:
            #logger = common.Logger("tg_crawl")
            pass

        #--0-- debug
        if step_name == 'debug':
	        #clear_tables_by_project(ID_PROJECT_main)
	        #cass_db.clear_table_by_project('git300_scrap.data_text', ID_PROJECT_main)
	        #cass_db.clear_table_by_project('git200_crawl.sn_activity', ID_PROJECT_main)
	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = 'govoritfursov', id_post = '')
	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '1436234144', id_post = '')
            #clear_tables_by_project(id_project = 1)
            #cass_db.clear_table_by_project('git200_crawl.sn_activity', id_project = 1)

            #import datetime
            #offset = datetime.timedelta(hours=3)
            #tz = datetime.timezone(offset, name='МСК')
            
            #dtutc = datetime.datetime.utcnow()
            #dt = datetime.datetime.now()

            #dtutc+offset == dt

            #dt11 = tz.fromutc(dtutc)

            tg_add_group(id_project = 1, name_group = 'meduzalive')
            #tg_crawl_messages_channel(id_project = 1, id_group = '1036240821', name_group = 'meduzalive', id_post = '', hash_group = '2994531093415596401') 
            #tg_crawl_messages_channel(id_project = 1, id_group = '', name_group = 'meduzalive', id_post = '', hash_group = '') 

            #tg_crawl_messages_channel(id_project = 1, id_group = '1430295016', name_group = 'AllDatingChe', id_post = '') 
            #tg_add_group(id_project = 1, name_group = 'govoritfursov')
            #tg_crawl_messages_channel(id_project = 1, id_group = '1436234144', name_group = 'govoritfursov', id_post = '') 
            #tg_crawl_messages_channel(id_project = 1, id_group = '', name_group = 'blogo', id_post = '') 
            #tg_crawl_messages_channel(id_project = 1, id_group = '', name_group = 'che_history', id_post = '') 
            #tg_crawl_messages_channel(id_project = 1, id_group = '', name_group = 'blogosfer', id_post = '') 

	        #cass_db.clear_table_by_project('git200_crawl.sn_accounts', ID_PROJECT_main)
	        #tg_crawl_groups(ID_PROJECT_main)

	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '1225634558', name_group = 'zhartwork', id_post = '') #

            #https://t.me/blogo
            #tg_add_group(id_project = 1, name_group = 'blogo') 

            #https://t.me/fursovchat
            #tg_add_group(id_project = 1, name_group = 'fursovchat') 

            #https://t.me/govoritfursov
            #tg_add_group(id_project = 1, name_group = 'govoritfursov') 

            #1430295016
            #tg_add_group(id_project = 1, name_group = 'AllDatingChe') 
            #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '1430295016', name_group = 'AllDatingChe', id_post = '')

	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '1342565932', name_group = '', id_post = '') #voljskiy_rabota
	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '', name_group = 'bmchel23', id_post = '')
	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '', name_group = 'mdpschel174', id_post = '') 

            #tg_add_group(id_project = 1, name_group = 'meduzalive')  
            #tg_add_group(id_project = 1, name_group = 'breakingmash')

	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '', name_group = 'chelyabinsky_znakomstva', id_post = '')
	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '1036240821', 
            #                      name_group = 'meduzalive', hash_group = '-2943065673075768629', id_post = '')
	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '', 
            #                       name_group = 'meduzalive', id_post = '')
	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '1036240821', 
            #                         name_group = 'meduzalive', hash_group = '2994531093415596401', id_post = '')

	        #tg_crawl_messages_channel(id_project = ID_PROJECT_main, id_group = '1156431022', name_group = 'Ali_boroda_74', id_post = '')
 	        #tg_crawler = tg.TelegramMessagesCrawler(debug_mode = DEBUG_MODE, 
            #									  msg_func = msg, #plpy.notice,
            #									  date_deep = None,
            #									  id_group = 1156431022,
            #									  name_group = 'Ali_boroda_74',
            #									  **accounts.TG_ACCOUNT[0])
 	        #tg_crawler.connect()
 	        #peer = tg_crawler.get_peer_entity('Ali_boroda_74', hash = -2081109350199989705)
            pass

        #--0-- clear
        if step_name == 'clear_all':
            #for i in range(1,20):
            #    clear_tables_by_project(i)
            #clear_tables_by_project(ID_PROJECT_main)
            pass

        #--1--
        if step_name == 'crawl_groups' or step_name == 'crawl_groups_upd_hash':
            update_hash = step_name == 'crawl_groups_upd_hash'
            cass_db.log_info('Start crawl groups '+step_name, ID_PROJECT_main,'')
            tg_crawl_groups(ID_PROJECT_main, job = job, update_hash = update_hash)
            pass

        #--2--
        if step_name == 'crawl_wall':
            cass_db.log_info('Start '+step_name, ID_PROJECT_main,'')

            #cass_db.clear_table_by_project('git300_scrap.data_text', ID_PROJECT_main)
            #cass_db.clear_table_by_project('git200_crawl.sn_activity', ID_PROJECT_main)

            if queue_generate:
                queue = crawler.QueueManager(id_source = TG_SOURCE_ID, id_project = ID_PROJECT_main, db = cass_db, min_subscribers=0)
                queue.regenerate()

            tg_crawl_messages_start(ID_PROJECT_main, queue)

except exceptions.StopProcess:
    #its ok  maybe user stop process
    pass
except Exception as e: 
    cass_db.log_fatal('CriticalErr on main_tg', ID_PROJECT_main, exceptions.get_err_description(e))
    raise


