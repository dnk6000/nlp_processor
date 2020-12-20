import time
from datetime import datetime

import CrawlModulesPG.const as const

class IntervalPauser:
    def __init__(self, delay_seconds):
        self.delay_seconds = delay_seconds
        self.dt_previous_action = datetime.now()

    def smart_sleep(self):
        _time_elapsed = (datetime.now() - self.dt_previous_action).total_seconds()
        if _time_elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - _time_elapsed)
        self.dt_previous_action = datetime.now()


class ExpPauser:
    def __init__(self, delay_seconds = 5.5, number_intervals = 4):
        # 5.5 sec ** 4 = ~ 15 min
        self.delay_seconds = 1.1 if delay_seconds <= 1 else delay_seconds
        self.number_intervals = number_intervals
        self.interval_counter = 0

    def sleep(self):
        self.interval_counter += 1
        if self.interval_counter > self.number_intervals:
            return False
        _sec_pause = self.delay_seconds ** self.interval_counter
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