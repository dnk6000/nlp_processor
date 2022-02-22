import modules.common_mod.const as const
import modules.crawling.date as date

from modules.db.pg import PgDb

class PgDbCassandra(PgDb):

    def __init__(self, plpy = None, GD = None):
        super().__init__(plpy, GD)

    def _execute(self, plan_id, var_list, id_project = 0):
        exept = None
        res = super()._execute(plan_id, var_list, exept)
        if exept is not None:
            self.log_error(const.CW_RESULT_TYPE_DB_ERROR, id_project, exept['description'])
        return res

    def _convert_select_result(self, res, str_to_date_conv_fields = [], decimal_to_float_conv_fields = []):

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
