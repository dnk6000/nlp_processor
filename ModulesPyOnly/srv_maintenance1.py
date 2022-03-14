from modules.db.cassandra import Cassandra, NeedStopChecker 
import modules.common_mod.const as const 
 
from modules.common_mod.globvars import GlobVars 
if const.PY_ENVIRONMENT:  
    GD = None 
else:  
    GD = {} 
gvars = GlobVars(GD) 
 
#################################################### 
####### begin: for PY environment only ############# 
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
 
def clear_tables_by_project(cass_db, id_project): 
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
        cass_db.query.clear_table_by_project(t, id_project) 


class Turn_off_doublegroups():
    def __init__(self, *args, db, projects_arr, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = db
        self.projects_arr = projects_arr
        self.projects_search_str = {}
        self.suitable_degree = 30

    def get_suitable_degree(self, acc):
        id_project = acc['id_project']

        if not id_project in self.projects_search_str:
            self.projects_search_str[id_project] = [_.strip().lower() for _ in acc['group_search_str'].split(",")]

        account_name = acc['account_name'].lower()
        account_screen_name = acc['account_screen_name'].lower()
        suitable_degree = self.suitable_degree

        for ss in self.projects_search_str[id_project]:
            if ss in account_name or ss in account_screen_name:
                suitable_degree = 100

        return suitable_degree

    def update_suitable_degree(self, id, suitable_degree):
        upd_select = f'\
            UPDATE \
              git200_crawl.sn_accounts \
            SET \
              suitable_degree = {str(suitable_degree)}::dmn.git_integer \
            WHERE \
              id = {str(id)}::dmn.git_pk \
            '
        res = self.db.free_query(upd_select)

    def turn_off_doublegroups(self): 
        res = self.db.git200_crawl.get_doubles_accounts(self.projects_arr)

        founded = []
        
        for acc in res:
            suitable_degree = self.get_suitable_degree(acc)

            if suitable_degree == 100:
                if acc['account_id'] in founded:
                    suitable_degree = self.suitable_degree + 20
                else:
                    founded.append(acc['account_id'])

            wow = '#########' if suitable_degree == 100 else ''
            print(f"acc_id: {acc['account_id']}  acc_name: {acc['account_name']}   acc_scr_name: {acc['account_screen_name']}   id_project: {acc['id_project']}   suitable_degree: {suitable_degree}   {wow}")

            if suitable_degree != acc['suitable_degree']:
                self.update_suitable_degree(acc['id'], suitable_degree)

        pass
 
def main(): 
    cass_db = Cassandra(plpy, GD) 
 
    ID_PROJECT = 16 # VK  Краснодар 
 
    #clear_tables_by_project(cass_db, ID_PROJECT) 
     
    dbg = Turn_off_doublegroups(db = cass_db, projects_arr = [9, 11, 14, 15, 16])
    dbg.turn_off_doublegroups()
    pass 

if __name__ == "__main__": 
    main() 
