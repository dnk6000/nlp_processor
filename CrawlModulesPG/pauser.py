import time
from datetime import (datetime, timedelta)

import CrawlModulesPG.const as const

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


if __name__ == "__main__":
    st = ExpPauser(delay_seconds = 2, number_intervals = 4)
    st.sleep()    
    st.sleep()    
    st.sleep()    
    st.sleep()    
    st.sleep()