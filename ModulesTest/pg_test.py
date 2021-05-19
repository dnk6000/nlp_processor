class MainDB:

    def __init__(self, plpy):
        self.db_error_counter = 0
        self.db_error_limit = 10  #number errors in a row (=подряд)
        self.plpy = plpy

    def test(self):
        self.plpy.notice('TEST PG SUCCESS')