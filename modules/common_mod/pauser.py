import time
from datetime import (datetime, timedelta, time as timedt)

import modules.common_mod.const as const

class IntervalPauser:
    '''Each subsequent pause is counted from the previous one 
    '''
    def __init__(self, delay_seconds = 2):
        self.delay_seconds = delay_seconds
        self.dt_previous_action = datetime.now()

    def smart_sleep(self):
        _time_elapsed = (datetime.now() - self.dt_previous_action).total_seconds()
        if _time_elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - _time_elapsed)
        self.dt_previous_action = datetime.now()


class ExpPauser:
    '''Exponential delay pauser
    '''
    def __init__(self, delay_seconds = 5.5, number_intervals = 4):
        # 5.5 sec ** 4 = ~ 15 min
        self.delay_seconds = 1.1 if delay_seconds <= 1 else delay_seconds
        self.number_intervals = number_intervals
        self.interval_counter = 0

    def _get_current_pause_sec(self):
        return self.delay_seconds ** self.interval_counter

    def _get_next_pause_sec(self):
        if self.interval_counter > self.number_intervals:
            return 0
        return self.delay_seconds ** (self.interval_counter + 1)

    def get_next_wake_up_date(self):
        self.interval_counter += 1
        if self.interval_counter > self.number_intervals:
            return None
        return datetime.now() + timedelta(seconds = self._get_current_pause_sec())

    def get_description(self):
        nps = self._get_next_pause_sec()
        s1 = 'Exponential pause. '+'Interval_counter: '+str(self.interval_counter+1)
        s2 = '  Pause sec: ' + str(nps)
        s3 = '  Wake up date: '+str(datetime.now() + timedelta(seconds = nps))
        return s1+s2+s3
        

    def sleep(self):
        self.interval_counter += 1
        if self.interval_counter > self.number_intervals:
            return False
        _sec_pause = self._get_current_pause_sec()
        time.sleep(_sec_pause)
        return True

    def reset(self):
        self.interval_counter = 0

class DayShedulePauser:
    '''24 hours is divided for pause or not-pause intervals  
       by default all intervals consider as not-pause intervals

       the sequence of intervals affects the result!

       from datetime import time as timedt
       pauser = DayShedulePauser()
       pauser.add_pause(timedt(9,0,0),timedt(23,59,59))  #pause from 9:00 to 23:59:59
       pauser.add_pause(timedt(4,30,0),timedt(5,0,0))    #pause from 4:30 to 5:00
    '''

    def __init__(self):
        day_start = timedt(0, 0, 0)
        day_end = timedt(23, 59, 59)

        self.intervals_set = [ {'beg': day_start, 'end': day_end, 'is_pause': False} ]

        self.seconds_for_check = 60 #frequency for check when need stop sleeping

    def add_pause(self, time_it_start, time_int_end):
        self.intervals_set.append({'beg': time_it_start, 'end': time_int_end, 'is_pause': True})

    def add_not_pause(self, time_it_start, time_int_end):
        self.intervals_set.append({'beg': time_it_start, 'end': time_int_end, 'is_pause': False})

    def need_sleep_now(self):
        now = datetime.now().time()

        need_sleep = False

        for interval in self.intervals_set:
            if interval['beg'] <= now and now <= interval['end']:
                need_sleep = interval['is_pause']
                #print(f"# {str(now)}  #  {str(interval['beg'])} - {str(interval['end'])} : {str(interval['is_pause'])}")

        return need_sleep

    def sleep_if_need(self):
        while self.need_sleep_now():
            #print('Sleeping '+str(datetime.now().time()))
            time.sleep(self.seconds_for_check)


if __name__ == "__main__":
    #st = ExpPauser(delay_seconds = 2, number_intervals = 4)
    #st.sleep()    
    #st.sleep()    
    #st.sleep()    
    #st.sleep()    
    #st.sleep()

    pauser = DayShedulePauser()
    pauser.seconds_for_check = 1
    h = 17
    m = 15
    pauser.add_pause(timedt(h,m,0),timedt(h,m,10))
    pauser.add_pause(timedt(h,m,20),timedt(h,m,30))
    pauser.add_pause(timedt(h,m,40),timedt(h,m,50))

    for i in range(120):
        print('working '+str(datetime.now().time()))
        time.sleep(1)
        pauser.sleep_if_need()