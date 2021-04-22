import time
import datetime
import json

import Crawling.vk as vk

import Common.pginterface as pginterface
import Common.const as const
import Crawling.crawler as crawler
import Crawling.scraper as scraper
import Crawling.exceptions as exceptions
import Crawling.date as date
import Crawling.accounts as accounts
import Crawling.pauser as pauser

from Crawling.globvars import GlobVars
if const.PY_ENVIRONMENT: 
    GD = None
else: 
    GD = {}
gvars = GlobVars(GD)


DEBUG_MODE = False

####### begin: for PY environment only #############
step_name = '_crawl_groups'
step_name = '_crawl_subscribers'
step_name = '_crawl_wall'
step_name = 'debug'
ID_TEST_PROJECT = 10

if const.PY_ENVIRONMENT:
    import CrawlingPyOnly.plpyemul as plpyemul
    import CrawlingPyOnly.self_psw as self_psw

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



def vk_crawl_groups(id_project, critical_error_counter = {'counter': 0}):

    need_stop_cheker = pginterface.NeedStopChecker(cass_db, id_project, 'crawl_group', state = 'off')

    vk_crawler = vk.CrawlerVkGroups(vk_account = accounts.VK_ACCOUNT[0], 
                         base_search_words = ['Челябинск'], 
                         msg_func = plpy.notice, 
                         id_project = id_project,
                         need_stop_cheker = need_stop_cheker,
                         )    
    select_result = cass_db.select_groups_id(id_project)
    vk_crawler.id_cash = list(i['account_id'] for i in select_result)

    for _res_unit in vk_crawler.crawl_groups(): #by API
        res_unit = json.loads(_res_unit)

        plpy.notice(res_unit['result_type'])

        if res_unit['result_type'] == const.CG_RESULT_TYPE_GROUPS_LIST:
            n = len(res_unit['groups_list'])
            c = 0
            plpy.notice('Add groups to DB: ' + str(n))

            for gr in res_unit['groups_list']:
                c += 1
                #plpy.notice('Add groups to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(gr['id']) + ' ' + gr['name'])
                cass_db.upsert_sn_accounts(gvars.get('VK_SOURCE_ID'), id_project, const.SN_GROUP_MARK,
                                 gr['id'], gr['name'], gr['screen_name'], gr['is_closed'] == 1 )
        
        #elif res_unit['result_type'] == const.CW_RESULT_TYPE_WARNING:
        #    cass_db.log_warn(res_unit['result_type'], id_project, res_unit['event_description'])
        #    if 'wall_processed' in res_unit:
        #        wall_processed = res_unit['wall_processed']
            
        elif res_unit['result_type'] == const.CW_RESULT_TYPE_ERROR:
            cass_db.log_error(res_unit['err_type'], id_project, res_unit['err_description'])
            plpy.notice(res_unit['err_type'])
            if res_unit['err_type'] in (const.ERROR_REQUEST_READ_TIMEOUT):
                plpy.notice('Request error: pause before repeating...')
                time.sleep(2) #TODO через параметр
                
        elif res_unit['result_type'] == const.CW_RESULT_TYPE_CRITICAL_ERROR:
            cass_db.log_fatal(res_unit['err_type'], id_project, res_unit['err_description'])
            critical_error_counter['counter'] += 1

            if res_unit['stop_process']:
                raise exceptions.StopProcess()

            if critical_error_counter['counter'] >= 3:
                raise exceptions.CrawlCriticalErrorsLimit(critical_error_counter)


def vk_crawl_wall(id_project, id_group, id_queue, 
                  project_params,
                  attempts_counter = 0, 
                  subscribers_only = False, 
                  id_post = '',
                  critical_error_counter = {'counter': 0}
                  ):

    if id_queue is not None:
        res = cass_db.queue_update(id_queue, date_start_process = date.date_now_str())
        if not res[0]['Success']:
            cass_db.log_error(const.LOG_LEVEL_ERROR, id_project, 'Error saving "git200_crawl.queue.{}" id_project = {} id = {}'.format('date_start_process', id_project, id_queue))

    sn_recrawler_checker = crawler.SnRecrawlerCheker(cass_db, 
                                                gvars.get('VK_SOURCE_ID'), 
                                                id_project, 
                                                sn_id = id_group, 
                                                recrawl_days_post = project_params['recrawl_days_post'], 
                                                recrawl_days_reply = project_params['recrawl_days_reply'],
                                                plpy = plpy)

    need_stop_cheker = pginterface.NeedStopChecker(cass_db, id_project, 'crawl_wall', state = 'off')

    request_error_pauser = pauser.ExpPauser()

    wall_processed = False
    CriticalErrorsLimit = 3

    id_group_str = str(id_group)

    dt_start = date.date_now_str()

    vk_crawler = vk.CrawlerVkWall(msg_func = plpy.notice, 
                                  subscribers_only = subscribers_only, 
                                  date_deep = project_params['date_deep'], 
                                  sn_recrawler_checker = sn_recrawler_checker,
                                  need_stop_cheker = need_stop_cheker,
                                  write_file_func = None,
                                  requests_delay_sec = project_params['requests_delay_sec'],
                                  request_error_pauser = request_error_pauser
                                  )
    vk_crawler._cw_set_debug_mode(turn_on = DEBUG_MODE, debug_post_filter = id_post)

    for res_list in vk_crawler.crawl_wall(id_group):
        _res_list = json.loads(res_list)
        n = len(_res_list)
        c = 0
        msg('Add posts to DB: ' + str(n))

        for res_unit in _res_list:
            c += 1

            msg(res_unit['result_type'])
            
            if res_unit['result_type'] == const.CW_RESULT_TYPE_NUM_SUBSCRIBERS:
                msg(res_unit['num_subscribers'])
                cass_db.update_sn_num_subscribers(gvars.get('VK_SOURCE_ID'), **res_unit)
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_NUM_SUBSCRIBERS_NOT_FOUND:
                cass_db.log_error(res_unit['result_type'], id_project, res_unit['event_description'])
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_DT_POST_ACTIVITY:
                msg('post id = {} dt = {}'.format(res_unit['post_id'], res_unit['dt']))
                cass_db.upsert_sn_activity(gvars.get('VK_SOURCE_ID'), id_project, id_group_str, res_unit['post_id'], res_unit['dt'], dt_start)
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_DT_GROUP_ACTIVITY:
                msg('dt = {}'.format(res_unit['dt']))
                cass_db.upsert_sn_activity(gvars.get('VK_SOURCE_ID'), id_project, id_group_str, '', res_unit['dt'], dt_start)
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_HTML:
                #msg('Add HTML to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['id']) + ' ' + res_unit['name'])
                res = cass_db.upsert_data_html(url = res_unit['url'], content = res_unit['content'], id_project = id_project, id_www_sources = gvars.get('VK_SOURCE_ID'))
                id_data_html = res['id_modified']
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_FINISH_NOT_FOUND:
                cass_db.log_error(res_unit['result_type'], id_project, res_unit['event_description'])
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_FINISH_SUCCESS:
                cass_db.log_trace(res_unit['result_type'], id_project, res_unit['event_description'])
                wall_processed = True
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_WARNING:
                cass_db.log_warn(res_unit['result_type'], id_project, res_unit['event_description'])
                if 'wall_processed' in res_unit:
                    wall_processed = res_unit['wall_processed']
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_ERROR:
                cass_db.log_error(res_unit['err_type'], id_project, res_unit['err_description'])
                msg(res_unit['err_type'])
                if res_unit['err_type'] in (const.ERROR_REQUEST_GET, const.ERROR_REQUEST_POST, const.ERROR_REQUEST_READ_TIMEOUT):
                    plpy.notice('Request error: pause before repeating...') #DEBUG
                    cass_db.log_info(const.LOG_INFO_REQUEST_PAUSE, id_project, request_error_pauser.get_description())
                    if not request_error_pauser.sleep():
                        raise exceptions.CrawlCriticalErrorsLimit(request_error_pauser.number_intervals)

                
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_CRITICAL_ERROR:
                cass_db.log_fatal(res_unit['err_type'], id_project, res_unit['err_description'])
                wall_processed = False
                critical_error_counter['counter'] += 1

                if False and id_queue is not None:  #this mechanism will be required when a problem is detected - one vk page is loaded, the other is not
                    attempts_counter += 1
                    date_deferred = datetime.datetime.now() + datetime.timedelta(minutes=30)
                    res = cass_db.queue_update(id_queue, attempts_counter = attempts_counter, date_deferred = date.date_to_str(date_deferred))
                    if not res[0]['Success']:
                        cass_db.log_error(const.LOG_LEVEL_ERROR, id_project, 'Error saving "git200_crawl.queue.{}" id_project = {} id = {}'.format('attempts_counter', id_project, id_queue))

                if res_unit['stop_process']:
                    raise exceptions.StopProcess()

                if critical_error_counter['counter'] >= CriticalErrorsLimit:  
                    raise exceptions.CrawlCriticalErrorsLimit(critical_error_counter)

            elif res_unit['result_type'] in (const.CW_RESULT_TYPE_POST, const.CW_RESULT_TYPE_REPLY, const.CW_RESULT_TYPE_REPLY_TO_REPLY):
                #msg('Add posts to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['id']) + ' ' + res_unit['name'])
                cass_db.upsert_data_text(id_data_html = id_data_html, id_project = id_project,  id_www_sources = gvars.get('VK_SOURCE_ID'),**res_unit)
                
    if id_queue is not None:
        res = cass_db.queue_update(id_queue, is_process = wall_processed, date_end_process = date.date_now_str())
        if not res[0]['Success']:
            cass_db.log_error(const.LOG_LEVEL_ERROR, id_project, 'Error saving "git200_crawl.queue.{}" id_project = {} id = {}'.format('date_end_process', id_project, id_queue))

def vk_crawl_wall_subscribers(id_project):
    select_result = cass_db.select_groups_id(id_project = id_project)
    project_params = cass_db.get_project_params(id_project)[0]

    number_of_groups = len(select_result)

    msg('Number groups for select subscribers: {}'.format(number_of_groups))

    c = 0
    portion_counter = 0
    critical_error_counter = {'counter': 0}

    PORTION_SIZE = 1

    while True:
        queue_portion = cass_db.queue_select(gvars.get('VK_SOURCE_ID'), id_project, PORTION_SIZE)

        portion_counter += 1
        msg('GET QUEUE PORTION № {}'.format(portion_counter));

        if len(queue_portion) == 0:
            break

        for elem in queue_portion:
            c += 1
            plpy.notice('Select subscribers for group: {}    Progress: {} / {}'.format(elem['sn_id'], c, number_of_groups))
            vk_crawl_wall(id_project = id_project, 
                          id_group = elem['sn_id'], 
                          id_queue = elem['id'], 
                          attempts_counter = elem['attempts_counter'], 
                          project_params = project_params, 
                          subscribers_only = True,
                          critical_error_counter = critical_error_counter)
            time.sleep(1)

def vk_crawling(id_project):

    portion_counter = 0
    critical_error_counter = {'counter': 0}

    while True:
        project_params = cass_db.get_project_params(id_project)[0]  #temporarily in the loop to adjust the pause 

        queue_portion = cass_db.queue_select(gvars.get('VK_SOURCE_ID'), id_project)

        portion_counter += 1
        plpy.notice('GET QUEUE PORTION № {}'.format(portion_counter));

        if len(queue_portion) == 0:
            break

        for elem in queue_portion:
            vk_crawl_wall(id_project = id_project, 
                          id_group = elem['sn_id'], 
                          id_queue = elem['id'], 
                          attempts_counter = elem['attempts_counter'], 
                          project_params = project_params,
                          critical_error_counter = critical_error_counter)

def vk_crawling_wall_group(id_project, id_group, id_post = ''):

    project_params = cass_db.get_project_params(id_project)[0]

    vk_crawl_wall(id_project = id_project, id_group = id_group, id_queue = None, attempts_counter = 0, project_params = project_params, id_post = id_post)
            
def msg(msgstr):
    if DEBUG_MODE:
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

def write_debug_file(msg):
    #print(msg)
    pass


#########################################


cass_db = pginterface.MainDB(plpy, GD)

cass_db.create_project(ID_TEST_PROJECT)

#--0-- debug
if step_name == 'debug':
    #clear_tables_by_project(ID_TEST_PROJECT)
    vk_crawling_wall_group(ID_TEST_PROJECT, id_group = '15158721')                       #debug group
    #vk_crawling_wall_group(ID_TEST_PROJECT, id_group = '87721351', id_post = '2359271')  #debug post

#--0-- clear
if step_name == 'clear_all':
    #for i in range(1,20):
    #    clear_tables_by_project(i)
    #clear_tables_by_project(ID_TEST_PROJECT)
    pass

#--1--
if step_name == 'crawl_groups':
    cass_db.log_info('Start '+step_name, ID_TEST_PROJECT,'')
    #vk_crawl_groups(ID_TEST_PROJECT)

#--2--
if step_name == 'crawl_subscribers':
    cass_db.log_info('Start '+step_name, ID_TEST_PROJECT,'')
    plpy.notice('GENERATE QUEUE id_project = {}'.format(ID_TEST_PROJECT));
    cass_db.clear_table_by_project('git200_crawl.queue', ID_TEST_PROJECT)
    cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT)
    vk_crawl_wall_subscribers(ID_TEST_PROJECT)

#--3--
if step_name == 'crawl_wall':
    cass_db.log_info('Start '+step_name, ID_TEST_PROJECT,'')
    
    plpy.notice('GENERATE QUEUE id_project = {}'.format(ID_TEST_PROJECT));
    cass_db.clear_table_by_project('git200_crawl.queue', ID_TEST_PROJECT)
    
    #cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT, 10001)
    #cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT, 5000, 10000)
    cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT, 1000, 5000)
    #cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT, 500, 1000)
    
    #cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT, 5000, 9999999)
    #cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT, 10, 5000)
    #cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT, 50, 100)
    #cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT, 0, 9)

    vk_crawling(ID_TEST_PROJECT)


#vk_crawl_wall(5, 52233236, subscribers_only = True)
#vk_crawl_wall(ID_TEST_PROJECT, 16758516, subscribers_only = False)

#vk_crawl_wall(130782889,subscribers_only = True)
#vk_crawl_wall(0, 222333444,subscribers_only = True)
#vk_crawl_wall_subscribers(0)
pass

