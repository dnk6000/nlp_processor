import asyncio
import time
import datetime

class AsyncCrawler:
    def __init__(self,  manager_func = None, 
                        debug_mode = True,
                        msg_func = None):
        self.manager_func = manager_func
        self.debug_mode = debug_mode
        self.tasks = []

    async def _crawling_task(self, sec):
        if self.debug_mode:
            await asyncio.sleep(sec)
        pass

    async def _manager(self, sec):
        if self.manager_func != None:
            self.manager_func()
        if self.debug_mode:
            await asyncio.sleep(sec)
        pass

    async def start(self):
        self.loop = asyncio.get_event_loop()
        
        _task1 = asyncio.Task(self._crawling_task(3))
        _task1.is_manager = False

        _task9 = asyncio.Task(self._manager(2))
        _task9.is_manager = True

        self.tasks = [ _task1, _task9 ]

        try:
            self.loop.run_until_complete(asyncio.wait(self.tasks))
        finally:
            self.loop.close()

    def msg(self, message):
        if not self.msg_func == None:
            try:
                self.msg_func(str(message))
            except:
                pass

    def debug_msg(self, message):
        if not self.debug_mode:
            return
        self.msg(message)




class TelegramCrawler:

    def __init__(self, func_save_to_db = None, msg_func = None):

        self.func_save_to_db = func_save_to_db
        self.msg_func = msg_func

        self.repeats = 20
        self.repeats2 = 10

        pass

    async def crawler(self, sec):
        while self.repeats > 0:
            self.repeats -= 1
            self.msg('crawling = {}  repeats = {}   time = {}'.format(sec, self.repeats, str(datetime.datetime.now())))
            await asyncio.sleep(sec)
        pass

    async def save_to_db(self):
        while self.repeats > 0:
            self.func_save_to_db()
            await asyncio.sleep(0.1)
            self.repeats2 -= 1
            if self.repeats2 <= 0:
                print('cancelling') 
                for task in self.tasks:
                    if not False and task.spec:
                        task.cancel()
                return
        pass  #self.tasks[0]._coro == TelegramCrawler.crawler(self, 2)    self.crawler         TelegramCrawler.crawler()

    def main(self):
        loop = asyncio.get_event_loop()
        #tasks = [
        #    asyncio.ensure_future(self.crawler(3)),
        #    asyncio.ensure_future(self.crawler(2)),
        #    asyncio.ensure_future(self.save_to_db()),
        #    ]
        self.t1 = asyncio.Task(self.crawler(3))
        self.t1.spec = True
        self.t2 = asyncio.Task(self.crawler(2))
        self.t2.spec = True
        self.t3 = asyncio.Task(self.save_to_db())
        self.t3.spec = False

        self.tasks = [
            self.t1,
            self.t2,
            self.t3
            ]
        loop.run_until_complete(asyncio.wait(self.tasks))
        loop.close()

        pass
    
    def msg(self, message):
        if self.msg_func != None:
            self.msg_func(message)


