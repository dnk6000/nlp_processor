import time
import datetime
import json

import CrawlModulesPG.vk as vk

import CrawlModulesPG.pginterface as pginterface
import CrawlModulesPG.const as const
import CrawlModulesPG.crawler as crawler
import CrawlModulesPG.scraper as scraper
import CrawlModulesPG.exceptions as exceptions

from CrawlModulesPG.globvars import GlobVars
if const.PY_ENVIRONMENT: 
    GD = None
else: 
    GD = {}
gvars = GlobVars(GD)


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




def vk_crawl_groups(id_project):

    import CrawlModulesPyOnly.self_psw as self_psw
    vk_crawler = vk.CrawlerVkGroups(login = '89273824101', 
                         password = self_psw.get_psw_vk_mtyurin(), 
                         base_search_words = ['Челябинск'], 
                         msg_func = plpy.notice, 
                         id_project = id_project
                         )
    select_result = cass_db.select_groups_id(id_project)
    vk_crawler.id_cash = list(i['account_id'] for i in select_result)

    for groups_list in vk_crawler.crawl_groups(): #by API
        _groups_list = json.loads(groups_list)

        n = len(_groups_list)
        c = 0
        print('Add groups to DB: ' + str(n))

        for gr in _groups_list:
            c += 1
            print('Add groups to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(gr['id']) + ' ' + gr['name'])
            cass_db.upsert_sn_accounts(gvars.get('VK_SOURCE_ID'), id_project, const.SN_GROUP_MARK,
                             gr['id'], gr['name'], gr['screen_name'], gr['is_closed'] == 1 )

def vk_crawl_wall(id_project, id_group, id_queue, 
                  project_params,
                  attempts_counter = 0, 
                  subscribers_only = False, 
                  id_post = '',
                  critical_error_counter = {'counter': 0}
                  ):

    if id_queue != None:
        res = cass_db.queue_update(id_queue, date_start_process = scraper.date_now_str())
        if not res[0]['Success']:
            cass_db.log_error(const.CW_LOG_LEVEL_ERROR, id_project, 'Error saving "git200_crawl.queue.{}" id_project = {} id = {}'.format('date_start_process', id_project, id_queue))

    sn_recrawler_checker = vk.SnRecrawlerCheker(cass_db, 
                                                gvars.get('VK_SOURCE_ID'), 
                                                id_project, 
                                                sn_id = id_group, 
                                                recrawl_days_post = project_params['recrawl_days_post'], 
                                                recrawl_days_reply = project_params['recrawl_days_reply'],
                                                plpy = plpy)

    need_stop_cheker = pginterface.NeedStopChecker(cass_db, id_project, 'crawl_wall', state = 'off')

    wall_processed = False

    id_group_str = str(id_group)

    dt_start = scraper.date_now_str()

    vk_crawler = vk.CrawlerVkWall(msg_func = plpy.notice, 
                                  subscribers_only = subscribers_only, 
                                  date_deep = project_params['date_deep'], 
                                  sn_recrawler_checker = sn_recrawler_checker,
                                  need_stop_cheker = need_stop_cheker,
                                  write_file_func = write_debug_file 
                                  )
    vk_crawler._cw_set_debug_mode(turn_on = True, debug_post_filter = id_post)

    for res_list in vk_crawler.crawl_wall(id_group):
        _res_list = json.loads(res_list)

        n = len(_res_list)
        c = 0
        plpy.notice('Add posts to DB: ' + str(n))

        for res_unit in _res_list:
            c += 1

            plpy.notice(res_unit['result_type'])
            
            if res_unit['result_type'] == const.CW_RESULT_TYPE_NUM_SUBSCRIBERS:
                plpy.notice(res_unit['num_subscribers'])
                cass_db.update_sn_num_subscribers(gvars.get('VK_SOURCE_ID'), **res_unit)
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_NUM_SUBSCRIBERS_NOT_FOUND:
                cass_db.log_error(res_unit['result_type'], id_project, res_unit['event_description'])
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_DT_POST_ACTIVITY:
                plpy.notice('post id = {} dt = {}'.format(res_unit['post_id'], res_unit['dt']))
                cass_db.upsert_sn_activity(gvars.get('VK_SOURCE_ID'), id_project, id_group_str, res_unit['post_id'], res_unit['dt'], dt_start)
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_DT_GROUP_ACTIVITY:
                plpy.notice('dt = {}'.format(res_unit['dt']))
                cass_db.upsert_sn_activity(gvars.get('VK_SOURCE_ID'), id_project, id_group_str, '', res_unit['dt'], dt_start)
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_HTML:
                #plpy.notice('Add HTML to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['id']) + ' ' + res_unit['name'])
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
                plpy.notice(res_unit['err_type'])
                if res_unit['err_type'] in (const.ERROR_REQUEST_GET, const.ERROR_REQUEST_POST):
                    plpy.notice('Request error: pause before repeating...')
                    time.sleep(2) #TODO через параметр
                
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_CRITICAL_ERROR:
                cass_db.log_fatal(res_unit['err_type'], id_project, res_unit['err_description'])
                wall_processed = False
                critical_error_counter['counter'] += 1

                if id_queue != None:
                    attempts_counter += 1
                    date_deferred = datetime.datetime.now() + datetime.timedelta(minutes=30)
                    res = cass_db.queue_update(id_queue, attempts_counter = attempts_counter, date_deferred = scraper.date_to_str(date_deferred))
                    if not res[0]['Success']:
                        cass_db.log_error(const.CW_LOG_LEVEL_ERROR, id_project, 'Error saving "git200_crawl.queue.{}" id_project = {} id = {}'.format('attempts_counter', id_project, id_queue))

                if res_unit['stop_process']:
                    raise exceptions.StopProcess()

                if critical_error_counter['counter'] >= 3:
                    raise exceptions.CrawlCriticalErrorsLimit(critical_error_counter)

            elif res_unit['result_type'] in (const.CW_RESULT_TYPE_POST, const.CW_RESULT_TYPE_REPLY, const.CW_RESULT_TYPE_REPLY_TO_REPLY):
                #plpy.notice('Add posts to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['id']) + ' ' + res_unit['name'])
                cass_db.upsert_data_text(id_data_html = id_data_html, id_project = id_project,  id_www_sources = gvars.get('VK_SOURCE_ID'),**res_unit)
                
    if id_queue != None:
        res = cass_db.queue_update(id_queue, is_process = wall_processed, date_end_process = scraper.date_now_str())
        if not res[0]['Success']:
            cass_db.log_error(const.CW_LOG_LEVEL_ERROR, id_project, 'Error saving "git200_crawl.queue.{}" id_project = {} id = {}'.format('date_end_process', id_project, id_queue))

def vk_crawl_wall_subscribers(id_project):
    select_result = cass_db.select_groups_id(id_project = id_project)
    project_params = cass_db.get_project_params(id_project)[0]

    number_of_groups = len(select_result)

    plpy.notice('Number groups for select subscribers: {}'.format(number_of_groups))

    c = 0
    portion_counter = 0
    critical_error_counter = {'counter': 0}

    while True:
        queue_portion = cass_db.queue_select(gvars.get('VK_SOURCE_ID'), id_project)

        portion_counter += 1
        plpy.notice('GET QUEUE PORTION № {}'.format(portion_counter));

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

    project_params = cass_db.get_project_params(id_project)[0]

    portion_counter = 0
    critical_error_counter = {'counter': 0}

    while True:
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

def vk_crawling_group(id_project, id_group, id_post = ''):

    project_params = cass_db.get_project_params(id_project)[0]

    vk_crawl_wall(id_project = id_project, id_group = id_group, id_queue = None, attempts_counter = 0, project_params = project_params, id_post = id_post)
            

def clear_tables_by_project(id_project):
    tables = [
        'git200_crawl.data_html',
        'git200_crawl.queue',
        'git200_crawl.sn_accounts',
        'git200_crawl.sn_activity',
        'git300_scrap.data_text',
        'git999_log.log'
        ]
    for t in tables:
        plpy.notice('Delete table {} by project {}'.format(t,id_project))
        cass_db.clear_table_by_project(t, id_project)

def write_debug_file(msg):
    #print(msg)
    pass


#########################################

ID_TEST_PROJECT = 7

cass_db = pginterface.MainDB(plpy, GD)

#--0-- debug

#from bs4 import BeautifulSoup
#import re
#debug_file = open(r"C:\Work\Python\CasCrawl37\Test\tst.txt", encoding='utf-8')
#msg = debug_file.read()
#debug_file.close()
#_cw_soup = BeautifulSoup(msg, "html.parser")
#res = _cw_soup.findAll(re.compile('.*'), { 'aria-label' : re.compile('(Подписчики)|(Участники)') })
#plpy.notice(str(res[0]))
#f=1
#import bs4
#plpy.notice('Version BS4 = '+bs4.__version__)

vk_crawling_group(ID_TEST_PROJECT, id_group = '87721351')                       #debug group
#vk_crawling_group(ID_TEST_PROJECT, id_group = '87721351', id_post = '2359271')  #debug post

#--0-- clear
#for i in range(1,20):
#    clear_tables_by_project(i)

#--1--
#vk_crawl_groups(ID_TEST_PROJECT)

#--2--
#plpy.notice('GENERATE QUEUE id_project = {}'.format(ID_TEST_PROJECT));
#cass_db.clear_table_by_project('git200_crawl.queue', ID_TEST_PROJECT)
#cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT)
#vk_crawl_wall_subscribers(ID_TEST_PROJECT)

#--3--
#plpy.notice('GENERATE QUEUE id_project = {}'.format(ID_TEST_PROJECT));
#cass_db.clear_table_by_project('git200_crawl.queue', ID_TEST_PROJECT)
#cass_db.queue_generate(gvars.get('VK_SOURCE_ID'), ID_TEST_PROJECT)
#vk_crawling(ID_TEST_PROJECT)


#vk_crawl_wall(5, 52233236, subscribers_only = True)
#vk_crawl_wall(ID_TEST_PROJECT, 16758516, subscribers_only = False)

#vk_crawl_wall(130782889,subscribers_only = True)
#vk_crawl_wall(0, 222333444,subscribers_only = True)
#vk_crawl_wall_subscribers(0)
pass

