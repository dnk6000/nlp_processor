import json

import vk

from plpyemul import PlPy
import pg_interface
import const

def get_psw_mtyurin():
    
    with open('C:\Temp\mypswvk.txt', 'r') as f:
        psw = f.read()

    return 'Bey'+psw+'00'


def vk_crawl_groups():

    id_project = 5

    crawler = vk.CrawlerVkGroups(login = '89273824101', 
                         password = get_psw_mtyurin(), 
                         base_search_words = ['Фурсов'], 
                         msg_func = PlPy.notice, 
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
            cass_db.add_to_db_sn_accounts(id_project,
                             'vk',
                             'group',
                             gr['id'],
                             gr['name'],
                             gr['screen_name'],
                             gr['is_closed'] == 1
            )

def vk_crawl_wall():
    id_project = 8

    crawler = vk.CrawlerVkWall(msg_func = PlPy.notice)

    for res_list in crawler.crawl_wall(16758516):
        _res_list = json.loads(res_list)

        n = len(_res_list)
        c = 0
        print('Add posts to DB: ' + str(n))

        for res_unit in _res_list:
            c += 1
            print(res_unit['result_type'])
            if res_unit['result_type'] == const.CW_RESULT_TYPE_NUM_SUBSCRIBERS:
                cass_db.update_sn_num_subscribers(id_project = id_project, sn_network = 'vk', **res_unit)
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_HTML:
                #print('Add HTML to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['id']) + ' ' + res_unit['name'])
                res = cass_db.add_to_db_data_html(id_project = id_project, domain = 'vk', sid = 1, **res_unit)   #sid ???
                gid_data_html = res['gid']
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_FINISH_NOT_FOUND:
                pass #!!!!!!!!!!!! запись ошибки в лог
                break
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_FINISH_SUCCESS:
                #print('FINISH success')
                pass #!!!!!!!!!!!! удалить страницу из очереди
                break
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_ERROR:
                print(res_unit['err_type'])
                pass #!!!!!!!!!!!! обработка ошибки в зависимости от ее типа - критичная/некритичная
            elif res_unit['result_type'] == const.CW_RESULT_TYPE_CRITICAL_ERROR:
                print(res_unit['err_type'])
                pass #!!!!!!!!!!!! обработка ошибки в зависимости от ее типа - критичная/некритичная
                break
            elif res_unit['result_type'] in (const.CW_RESULT_TYPE_POST, const.CW_RESULT_TYPE_REPLY, const.CW_RESULT_TYPE_REPLY_TO_REPLY):
                #print('Add posts to DB: ' + str(c) + ' / ' + str(n) + '  ' + str(res_unit['id']) + ' ' + res_unit['name'])
                cass_db.add_to_db_data_text(id_project = id_project, sn_network = 'vk', gid_data_html = gid_data_html, **res_unit)

    #res = Crawler.crawl_wall('16758516_109038')
    pass



#########################################
cass_db = pg_interface.MainDB()
#vk_crawl_groups()
vk_crawl_wall()

