# -*- coding: utf-8 -*-

import re
import datetime

import Modules.Common.const as const

class StrToDate:
    '''re_patterns - 're'-patterns str or list with  str:
        - 'dd mmm в hh:mm' ~ 20 ноя в 12:30
        - 'dd mmm yyyy' ~ 20 ноя 2019
        url, msg_func - for error exceptions
        str_date_format - format str from 'datetime' library
    '''
    MONTHSHORTSt = ['янв','фев','мар','апр','мая','июн','июл','авг','сен','окт','ноя','дек']
    MONTHSHORTSs = '|'.join(MONTHSHORTSt)
    REPATTERNS = {
        'dd mmm в hh:mm' : '(?P<day>\d\d?) (?P<monthshort>'+MONTHSHORTSs+') в (?P<hour>\d\d?):(?P<minute>\d\d)', #15 янв в 10:40
        'dd mmm yyyy'    : '(?P<day>\d\d?) (?P<monthshort>'+MONTHSHORTSs+') (?P<year>\d\d\d\d)',                 #15 янв 2019
        'сегодня в hh:mm': '(?P<day>сегодня) в (?P<hour>\d\d?):(?P<minute>\d\d)',                                #сегодня в 10:40
        'вчера в hh:mm'  : '(?P<day>вчера) в (?P<hour>\d\d?):(?P<minute>\d\d)',                                  #вчера в 10:40
        '%Y-%m-%d %H:%M:%S+.*': '(?P<year>\d\d\d\d?)-(?P<month>\d\d?)-(?P<day>\d\d?) (?P<hour>\d\d?):(?P<minute>\d\d?):(?P<second>\d\d?)' #2020-12-05 07:58:11+03
        }

    def __init__(self, re_patterns = '', url = '', msg_func = None, str_date_format = ''):
        if str_date_format =='':
            self.str_date_format = "%d.%m.%Y %H:%M:00"
        else:
            self.str_date_format = str_date_format
        self.allowed_re_patterns = {}
        self.url = url 
        self.msg_func = msg_func 

        if isinstance(re_patterns, str):
            if re_patterns == '':
                for i in self.REPATTERNS:
                    self.allowed_re_patterns[i] = re.compile(self.REPATTERNS[i])
            else:
                self.allowed_re_patterns[re_patterns] = re.compile(self.REPATTERNS[re_patterns])

        elif isinstance(re_patterns, list) or isinstance(re_patterns, tuple):
            for i in re_patterns:
                self.allowed_re_patterns[i] = re.compile(self.REPATTERNS[i])

    def get_date(self, date_in_str, type_res = 'S'):
        _res_date_in_datetime = const.EMPTY_DATE 
        _td = datetime.timedelta(days = 0)

        for re_pattern in self.allowed_re_patterns:  
            match = self.allowed_re_patterns[re_pattern].match(date_in_str)
            if match:
                res = match.groupdict()
                dt_now = datetime.datetime.now()
                
                if 'monthshort' in res:
                    month = int(self.MONTHSHORTSt.index(match.group('monthshort'))) + 1
                elif 'month' in res:
                    month = int(match.group('month'))
                else:
                    month = 1

                if 'day' in res:
                    if match.group('day') == 'сегодня' or match.group('day') == 'вчера':
                        day = dt_now.day
                        month = dt_now.month
                        if match.group('day') == 'вчера':
                            _td = datetime.timedelta(days = -1)
                    else:
                        day = int(match.group('day'))
                else:
                    day = 1
                
                if 'year' in res:
                    year = int(match.group('year'))
                else:
                    year = dt_now.year
                    _cur_month = dt_now.month
                    _cur_day = dt_now.day
                    if (month > _cur_month) or (month == _cur_month and day > _cur_day): #
                        year -= 1

                hour   = 0 if not 'hour'   in res else int(match.group('hour'))
                minute = 0 if not 'minute' in res else int(match.group('minute'))

                try:
                    _res_date_in_datetime = datetime.datetime(year, month, day, hour, minute) + _td
                except:
                    raise exceptions.ScrapeDateError(self.url, 'Error by scraping date from str "'+date_in_str+'"', self.msg_func)

                break

        _res_date_in_str = self._get_formated_date(_res_date_in_datetime)

        if type_res == 'S,D':
            return _res_date_in_str, _res_date_in_datetime
        elif type_res == 'D':
            return _res_date_in_datetime
        else:
            return _res_date_in_str

    def _get_formated_date(self, dt):
        if self.str_date_format == '':
            return dt
        else:
            return dt.strftime(self.str_date_format)  

def date_to_str(dt):
    return dt.strftime("%d.%m.%Y %H:%M:%S")

def date_now_str():
    return date_to_str(datetime.datetime.now().astimezone())

def date_as_utc(dt):
    if dt is None:
        return const.EMPTY_DATE_UTC
    elif dt.tzinfo is not None:
        return dt
    elif dt == const.EMPTY_DATE:
        return const.EMPTY_DATE_UTC
    else:
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo = datetime.timezone.utc)

if __name__ == "__main__":
    st = StrToDate()
    d = st.get_date('08 янв в 10:43')
    print(d)
