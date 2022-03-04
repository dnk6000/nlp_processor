import modules.common_mod.const as const
import modules.common_mod.date as date
import modules.common_mod.exceptions as exceptions

from modules.db.pg import PgDb

class PgDbCassandra(PgDb):

    def __init__(self, plpy = None, GD = None):
        super().__init__(plpy, GD)

    def _execute(self, plan_id, var_list, id_project = 0, fix_log_on_expt = True):
        exept = {}
        try:
            res = super()._execute(plan_id, var_list, exept)
        except Exception as expt:
            if hasattr(self,'db'):
                self.rollback()
                self.db.git999_log.log_fatal(const.CW_RESULT_TYPE_DB_ERROR, id_project, description=exceptions.get_err_description(expt, id_project = id_project, var_list = var_list))
            raise

        if len(exept) != 0:
            if fix_log_on_expt and hasattr(self,'db'):
                self.db.git999_log.log_error(const.CW_RESULT_TYPE_DB_ERROR, id_project, description=exept['description'])
            else:
                self.notice('Exeption raised: '+exept['description'])
        return res

    def _convert_select_result(self, res, str_to_date_conv_fields = [], decimal_to_float_conv_fields = []):

        if res is None:
            return res

        #plpy in PG return date fields in str format, therefore, a conversion is required
        if const.PG_ENVIRONMENT and len(str_to_date_conv_fields) > 0:

            converter = date.StrToDate('%Y-%m-%d %H:%M:%S+.*')

            for field in str_to_date_conv_fields:
                for row in res:
                    row[field] = converter.get_date(row[field], type_res = 'D')
                
        if len(decimal_to_float_conv_fields) > 0:
            for field in decimal_to_float_conv_fields:
                for row in res:
                    row[field] = row[field].__float__()  #class Decimal to float

        #return [row[0] for row in res]
        return res
