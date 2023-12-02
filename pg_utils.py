from config import *
import psycopg2.extras
from psycopg2.extras import Json
from functools import wraps
import time


def retry(fn=None):
    @wraps(fn)
    def wrapper(*args, **kw):
        cls = args[0]
        while True:
            try:
                return fn(*args, **kw)
            except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                print(e)
                print ("\nDatabase Connection [InterfaceError or OperationalError]")
                print ("Idle for %s seconds" % (2))
                time.sleep(2)
                cls._connect()
    return wrapper

class pg_utils:

    def __init__(self,connname="testdbconn"):
        self.conn = None
        self.cur1 = None
        self.cur2 = None
        self.cur3 = None
        self.connname=connname

        self._connect()

    def _connect(self):
        self.conn = psycopg2.connect(application_name=self.connname, database=database, user=user, password=password, host=host, port=port)
        self.cur1 = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.cur2 = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.cur3 = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def __del__(self):
        # Maybe there is a connection but no cursor, whatever close silently!
        try:
            self.conn.close()
        except:
            pass
        self.conn = None

    @retry
    def cur1_create(self):
        if self.cur1 == None or self.cur1.closed:
            self.cur1 = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return True
    
    @retry
    def cur2_create(self):
        if self.cur2 == None or self.cur2.closed:
            self.cur2 = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return True
    
    @retry
    def cur3_create(self):
        if self.cur3 == None or self.cur3.closed:
            self.cur3 = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return True

    def cur1_close(self):
        self.cur1.close()
        self.cur1=None
        return True
    
    def cur2_close(self):
        self.cur2.close()
        self.cur2=None
        return True
    
    def cur3_close(self):
        self.cur3.close()
        self.cur3=None
        return True
    
    @retry
    def cur1_execute(self,sql,params=None): # pass here required params to get data from DB
        return self.cur1.execute(sql,params)

    @retry
    def cur1_fetchone(self): # pass here required params to get data from DB
        return self.cur1.fetchone()
    
    @retry
    def cur1_fetchall(self): # pass here required params to get data from DB
        return self.cur1.fetchall()
    
    @retry
    def cur2_execute(self,sql,params=None): # pass here required params to get data from DB
        return self.cur2.execute(sql,params)
    
    @retry
    def cur2_fetchone(self): # pass here required params to get data from DB
        return self.cur2.fetchone()

    @retry
    def cur3_execute(self,sql,params=None): # pass here required params to get data from DB
        return self.cur3.execute(sql,params)
    
    @retry
    def cur3_fetchone(self): # pass here required params to get data from DB
        return self.cur3.fetchone()

    @retry
    def conn_commit(self): 
        return self.conn.commit()

        
        
