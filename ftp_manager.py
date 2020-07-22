import time
import threading
import ftplib



FTP_BASE = "ftp.ncbi.nlm.nih.gov"




class FTPConnection():
    
    def __init__(self, uri):
        self.lock = threading.Lock()
        #lock until initialization complete
        self.lock.acquire()
        #initialize connection in thread, should provide speedup since network bound
        t = threading.Thread(target = self.create_connection, args = (uri, self.lock.release))
        t.start()
        
        self.uri = uri

    def create_connection(uri, cb = None):
        self.ftp = ftplib.FTP(uri)
        self.ftp.login()

    #replace ftp with new connection, note should be locked before calling
    def reconnect(self, con):
        #try to quit the connection in case still active
        self.disconnect()
        create_connection(self.uri)

    def locked(self):
        return self.lock.locked()

    def disconnect(self):
        try:
            self.ftp.quit()
        except ftplib.all_errors:
            pass

    def acquire(self, blocking = True):
        return self.lock.acquire(blocking)
    
    def release(self):
        return self.lock.release()

    def __enter__(self):
        self.acquire()
    
    def __exit__(self, type, value, tb):
        self.release()



class FTPManager:
    MIN_CONS = 10
    MAX_CONS = 1000
    #if only 5 connections left prepare some more
    BUFFER_CONS = 5
    #if more than 10 connections to spare start removing some
    PRUNE_BUFFER = 10
    MIN_HEARTRATE = 1
    

    def __init__(self, uri, init_cons = MIN_CONS, heartrate = MIN_HEARTRATE):
        if init_cons < MIN_CONS:
            init_cons = MIN_CONS
        elif init_cons > MAX_CONS:
            init_cons = MAX_CONS
        if heartrate <= MIN_HEARTRATE:
            heartrate = MIN_HEARTRATE

        self.heartrate = heartrate

        self.uri = uri
        self.in_use = 0
        self.in_use_lock = threading.Lock()

        self.connections = {FTPConnection(uri) for i in range(init_cons)}

        self.ended = threading.Event()
        self.all_busy = threading.Semaphore(MAX_CONS)
        self.acquire_lock = threading.Lock()
        self.connections_lock = threading.lock()
        #heartbeat thread to ensure connections stay alive
        heartbeat = threading.Thread(target = __heartbeat, daemon = True)
        #start heartbeat thread
        heartbeat.start()


    #(ftp, lock)
    def __check_pulse(self, con, force = False):
        #if connection in use, just pass heartbeat check, unless told to force (assumes ok to unlock after)
        if not con.acquire(False) and not force:
            return
        #connection acquired (locked)
        #make sure manager wasn't ended while doing checks
        if ended.is_set():
            return
        try:
            con.ftp.voidcmd("NOOP")
        except ftplib.all_errors:
            con.reconnect(con)
        #release connection (unlock)
        con.release()



    def __heartbeat(self):
        taken = 0
        while not ended.is_set():
            sleep_for = max(HEARTRATE - taken, 0)
            time.sleep(sleep_for)
            #check if was set while sleeping
            if not ended.is_set():
                #how long will this take to run? can the heartbeat be done on a single thread?
                start = time.time()
                for connection in connections:
                    #if all busy then obviously don't need heartbeat, just skip
                    if self.all_busy.acquire(False):
                        check_pulse(connection)
                        self.all_busy.release()
                taken = time.time() - start



    def end_all(self):
        if ended.is_set:
            return
        #no more operations
        self.ended.set()
        for con in connections:
            #need to wait until not busy
            with con:
                con.disconnect()
        

    def get_con(self):
        #return none if
        if self.ended.is_set():
            return None
        #block if maximum number of connections are busy
        self.all_busy.acquire():
        self.in_use_lock.acquire()
        self.in_use += 1
        self.in_use_lock.release()

        available = None
        #get connections lock
        self.connections_lock.acquire()
        for con in connections:
            #try to acquire the connection, move on if already locked
            if con.acquire(False):
                #connection acquired, ready for use by caller
                available = con
                break
        self.connections_lock.release()
        self.check_add_connection()
        return con
            


    def release_con(self, con, problem = False):
        self.all_busy.release()
        self.in_use_lock.acquire()
        self.in_use -= 1
        self.in_use_lock.release()
        pruned = check_prune_connection(con)
        #don't bother fixing potentially broken connections if not pruned
        #caller indicated a possible problem with the connection
        if not pruned and problem
            #lock currently held, force pulse check (this will handle unlocking)
            self.__check_pulse(con, True)
        else:
            con.release()

        
    def check_add_connection(self):
        self.connections_lock.acquire()
        self.in_use_lock.acquire()
        #check if idle connections is less than the number of buffer connections and total connections less than max
        if len(self.connections) - self.in_use < BUFFER_CONS and len(self.connections) < MAX_CONS:
            self.in_use_lock.release()
            self.add_connection()
        else:
            self.in_use_lock.release()
        self.connections_lock.release()

    def add_connection(self):
        con = FTPConnection(self.uri)
        with self.connections_lock:
            self.connections.add(con)



    #no need to manage a ton of connections if most of them are idle
    def check_prune_connection(self, con):
        pruned = False
        #don't need to do this
        if ended.is_set:
            return
        self.connections_lock.acquire()
        self.in_use_lock.acquire()
        #check if idle connections exceeds prune buffer and make sure not to prune past minimum number of connections
        if len(self.connections) - self.in_use > PRUNE_BUFFER and len(self.connections) > MIN_CONS:
            #don't need in use value anymore, can release
            self.in_use_lock.release()
            self.prune_connection()
            pruned = True
        else:
            self.in_use_lock.release()
        self.connections_lock.release()
        return pruned

    #connections should be pre-locked
    def prune_connection(self, con):
        con.disconnect()
        connections.remove(con)




    








    