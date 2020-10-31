import time
import datetime
import json

import vk

from plpyemul import PlPy as plpy
import pg_interface
import const
import gvars
import scraper

VK_SOURCE_ID = gvars.get('VK_SOURCE_ID')

def get_psw_mtyurin():
    
    with open('C:\Temp\mypswvk.txt', 'r') as f:
        psw = f.read()

    return 'Bey'+psw+'00'


def vk_crawl_groups():

    id_project = 5

    crawler = vk.CrawlerVkGroups(login = '89273824101', 
                         password = get_psw_mtyurin(), 
                         base_search_words = ['Фурсов'], 
                         msg_func = plpy.notice, 
                         id_project = id_project
                         )
    select_result = cass_db.select_groups_id(id_project = 5)
    crawler.id_cash = list(i['account_id'] for i in select_result)

    for groups_list in crawler.crawl_groups(): #by API
        _groups_list = json.loads(groups_list)

        n = len(_groups_list)
        c = 0
        print('Add groups to DB: ' + str(n))

        for gr in _groups_list:
            c += 1
            print('Add groups to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(gr['id']) + ' ' + gr['name'])
            cass_db.upsert_sn_accounts(gvars.get('VK_SOURCE_ID'), id_project, const.SN_GROUP_MARK,
                             gr['id'], gr['name'], gr['screen_name'], gr['is_closed'] == 1 )

def vk_crawl_wall(id_project, id_group, id_queue, attempts_counter = 0, subscribers_only = False):

    res = cass_db.queue_update(id_queue, date_start_process = scraper.date_now_str())
    if not res[0]['Success']:
        cass_db.log_error(const.CW_LOG_LEVEL_ERROR, id_project, 'Error saving "git200_crawl.queue.{}" id_project = {} id = {}'.format('date_start_process', id_project, id_queue))

    wall_processed = False

    id_group_str = str(id_group)

    crawler = vk.CrawlerVkWall(msg_func = plpy.notice, subscribers_only = subscribers_only)

    for res_list in crawler.crawl_wall(id_group):
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
                cass_db.upsert_sn_activity(gvars.get('VK_SOURCE_ID'), res_unit['post_id'], res_unit['dt'])
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_DT_GROUP_ACTIVITY:
                plpy.notice('dt = {}'.format(res_unit['dt']))
                cass_db.upsert_sn_activity(gvars.get('VK_SOURCE_ID'), id_group_str, res_unit['dt'])
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_HTML:
                #plpy.notice('Add HTML to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['id']) + ' ' + res_unit['name'])
                res = cass_db.upsert_data_html(url = res_unit['url'], content = res_unit['content'], id_project = id_project, id_www_sources = gvars.get('VK_SOURCE_ID'))
                id_data_html = res['id_modified']
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_FINISH_NOT_FOUND:
                cass_db.log_error(res_unit['result_type'], id_project, res_unit['event_description'])
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_FINISH_SUCCESS:
                cass_db.log_trace(res_unit['result_type'], id_project, res_unit['event_description'])
                wall_processed = True
            
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_ERROR:
                cass_db.log_error(res_unit['err_type'], id_project, res_unit['err_description'])
                plpy.notice(res_unit['err_type'])
                if res_unit['err_type'] in (const.ERROR_REQUEST_GET, const.ERROR_REQUEST_POST):
                    plpy.notice('Request error: pause before repeating...')
                    time.sleep(2) #через параметр
                
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_CRITICAL_ERROR:
                cass_db.log_fatal(res_unit['err_type'], id_project, res_unit['err_description'])
                plpy.notice(res_unit['err_type'])
                plpy.notice(res_unit['err_description'])
                wall_processed = False

                attempts_counter += 1
                date_deferred = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=30)
                res = cass_db.queue_update(id_queue, attempts_counter = attempts_counter, date_deferred = scraper.date_to_str(date_deferred))
                if not res[0]['Success']:
                    cass_db.log_error(const.CW_LOG_LEVEL_ERROR, id_project, 'Error saving "git200_crawl.queue.{}" id_project = {} id = {}'.format('attempts_counter', id_project, id_queue))

            
            elif res_unit['result_type'] in (const.CW_RESULT_TYPE_POST, const.CW_RESULT_TYPE_REPLY, const.CW_RESULT_TYPE_REPLY_TO_REPLY):
                #plpy.notice('Add posts to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['id']) + ' ' + res_unit['name'])
                cass_db.upsert_data_text(id_data_html = id_data_html, id_project = id_project,  id_www_sources = gvars.get('VK_SOURCE_ID'),**res_unit)

    res = cass_db.queue_update(id_queue, is_process = wall_processed, date_end_process = scraper.date_now_str())
    if not res[0]['Success']:
        cass_db.log_error(const.CW_LOG_LEVEL_ERROR, id_project, 'Error saving "git200_crawl.queue.{}" id_project = {} id = {}'.format('date_end_process', id_project, id_queue))

def vk_crawl_wall_subscribers(id_project):
    select_result = cass_db.select_groups_id(id_project = id_project)

    number_of_groups = len(select_result)

    plpy.notice('Number groups for select subscribers: {}'.format(number_of_groups))

    c = 0

    for i in select_result:
        c += 1
        sn_id = i['account_id']
        plpy.notice('Select subscribers for group: {}    Progress: {} / {}'.format(sn_id, c, number_of_groups))
        vk_crawl_wall(id_project, sn_id, subscribers_only = True)
        time.sleep(1)

def vk_crawling(id_project):

    portion_counter = 0

    while True:
        queue_portion = cass_db.queue_select(VK_SOURCE_ID, id_project)

        portion_counter += 1
        plpy.notice('GET QUEUE PORTION № {}'.format(portion_counter));

        if len(queue_portion) == 0:
            break

        for elem in queue_portion:
            vk_crawl_wall(id_project, elem['sn_id'], elem['id'], elem['attempts_counter'])
            


#########################################

ID_TEST_PROJECT = 5

cass_db = pg_interface.MainDB()

vk_crawling(ID_TEST_PROJECT)

#vk_crawl_groups()
vk_crawl_wall(5, 52233236, subscribers_only = True)
#vk_crawl_wall(5, 16758516, subscribers_only = False)

#vk_crawl_wall(130782889,subscribers_only = True)
#vk_crawl_wall(0, 222333444,subscribers_only = True)
#vk_crawl_wall_subscribers(0)

