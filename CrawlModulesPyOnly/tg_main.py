import datetime
import time
import json

import CrawlModulesPG.const as const
import CrawlModulesPG.date as date
import CrawlModulesPG.tg as tg
import CrawlModulesPG.pginterface as pginterface
import CrawlModulesPyOnly.plpyemul as plpyemul
import CrawlModulesPG.accounts as accounts
import CrawlModulesPG.scraper as scraper

from CrawlModulesPG.globvars import GlobVars
if const.PY_ENVIRONMENT: 
    GD = None
else: 
    GD = {}
gvars = GlobVars(GD)


DEBUG_MODE = True

####### begin: for PY environment only #############
step_name = '_crawl_groups'
step_name = '_crawl_subscribers'
step_name = '_crawl_wall'
step_name = 'debug'
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


def tg_crawl_messages(id_project, id_group, id_queue, 
                  project_params,
                  attempts_counter = 0, 
                  critical_error_counter = {'counter': 0}
                  ):
    
    dt_start = date.date_now_str()

    tg_crawler = tg.TelegramMessagesCrawler(debug_mode = DEBUG_MODE, 
                                            msg_func = plpy.notice, 
                                            id_group = id_group,
                                            date_deep = project_params['date_deep'],
                                            requests_delay_sec = project_params['requests_delay_sec'],
                                            **accounts.TG_ACCOUNT[0])
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
                #cass_db.update_sn_num_subscribers(gvars.get('VK_SOURCE_ID'), **res_unit)
            
            #elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_NUM_SUBSCRIBERS_NOT_FOUND:
            #    cass_db.log_error(res_unit['result_type'], id_project, res_unit['event_description'])
            
            elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_DT_POST_ACTIVITY:
                msg('post id = {} dt = {}'.format(res_unit['sn_post_id'], res_unit['last_date']))
                cass_db.upsert_sn_activity(TG_SOURCE_ID, id_project, upd_date = dt_start, **res_unit) 

            #elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_DT_GROUP_ACTIVITY:
            #    msg('dt = {}'.format(res_unit['dt']))
            #    cass_db.upsert_sn_activity(gvars.get('VK_SOURCE_ID'), id_project, id_group_str, '', res_unit['dt'], dt_start)
            
            #elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_HTML:
            #    #msg('Add HTML to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['id']) + ' ' + res_unit['name'])
            #    res = cass_db.upsert_data_html(url = res_unit['url'], content = res_unit['content'], id_project = id_project, id_www_sources = gvars.get('VK_SOURCE_ID'))
            #    id_data_html = res['id_modified']
            
            #elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_FINISH_NOT_FOUND:
            #    cass_db.log_error(res_unit['result_type'], id_project, res_unit['event_description'])
            
            #elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_FINISH_SUCCESS:
            #    cass_db.log_trace(res_unit['result_type'], id_project, res_unit['event_description'])
            #    wall_processed = True
            
            #elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_WARNING:
            #    cass_db.log_warn(res_unit['result_type'], id_project, res_unit['event_description'])
            #    if 'wall_processed' in res_unit:
            #        wall_processed = res_unit['wall_processed']
            
            #elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_ERROR:
            #    cass_db.log_error(res_unit['err_type'], id_project, res_unit['err_description'])
            #    msg(res_unit['err_type'])
            #    if res_unit['err_type'] in (const.ERROR_REQUEST_GET, const.ERROR_REQUEST_POST, const.ERROR_REQUEST_READ_TIMEOUT):
            #        plpy.notice('Request error: pause before repeating...') #DEBUG
            #        cass_db.log_info(const.LOG_INFO_REQUEST_PAUSE, id_project, request_error_pauser.get_description())
            #        if not request_error_pauser.sleep():
            #            raise exceptions.CrawlCriticalErrorsLimit(request_error_pauser.number_intervals)

                
            #elif res_unit['result_type'] == scraper.ScrapeResult.RESULT_TYPE_CRITICAL_ERROR:
            #    cass_db.log_fatal(res_unit['err_type'], id_project, res_unit['err_description'])
            #    wall_processed = False
            #    critical_error_counter['counter'] += 1

            #    if False and id_queue is not None:  #this mechanism will be required when a problem is detected - one vk page is loaded, the other is not
            #        attempts_counter += 1
            #        date_deferred = datetime.datetime.now() + datetime.timedelta(minutes=30)
            #        res = cass_db.queue_update(id_queue, attempts_counter = attempts_counter, date_deferred = date.date_to_str(date_deferred))
            #        if not res[0]['Success']:
            #            cass_db.log_error(scraper.ScrapeResult.LOG_LEVEL_ERROR, id_project, 'Error saving "git200_crawl.queue.{}" id_project = {} id = {}'.format('attempts_counter', id_project, id_queue))

            #    if res_unit['stop_process']:
            #        raise exceptions.StopProcess()

            #    if critical_error_counter['counter'] >= CriticalErrorsLimit:  
            #        raise exceptions.CrawlCriticalErrorsLimit(critical_error_counter)

            elif res_unit['result_type'] in (scraper.ScrapeResult.RESULT_TYPE_POST, scraper.ScrapeResult.RESULT_TYPE_REPLY, scraper.ScrapeResult.RESULT_TYPE_REPLY_TO_REPLY):
                msg('Add posts to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['sn_id']) + ' ' + res_unit['url'])
                cass_db.upsert_data_text(id_data_html = 0, id_project = id_project,  id_www_sources = TG_SOURCE_ID, **res_unit)
    pass
                                  

def tg_crawl_messages_start(id_project):

    critical_error_counter = {'counter': 0}

    project_params = cass_db.get_project_params(id_project)[0]          #DEBUG
    tg_crawl_messages(id_project = id_project,                          #DEBUG
                    id_group = 'govoritfursov',
                    id_queue = 0,
                    project_params = project_params,
                    critical_error_counter = critical_error_counter)


    return #DEBUG

    portion_counter = 0

    while True:
        project_params = cass_db.get_project_params(id_project)[0]  #temporarily in the loop to adjust the pause 

        queue_portion = cass_db.queue_select(gvars.get('VK_SOURCE_ID'), id_project)

        portion_counter += 1
        plpy.notice('GET QUEUE PORTION № {}'.format(portion_counter));

        if len(queue_portion) == 0:
            break

        for elem in queue_portion:
            tg_crawl_messages(id_project = id_project, 
                          id_group = elem['sn_id'], 
                          id_queue = elem['id'], 
                          attempts_counter = elem['attempts_counter'], 
                          project_params = project_params,
                          critical_error_counter = critical_error_counter)

def msg(msgstr):
    if DEBUG_MODE:
        plpy.notice(msgstr)


cass_db = pginterface.MainDB(plpy, GD)
TG_SOURCE_ID = gvars.get('TG_SOURCE_ID')

cass_db.create_project(ID_PROJECT_main)

tg_crawl_messages_start(ID_PROJECT_main)


