
POWER_AVG = 1000 #In instructions per second

REQ_PERIOD_MIN=30
REQ_PERIOD_MAX=300

WANTED_TASK_FACTOR = 3

class Client:
    def __init__(self,identifier,power=POWER_AVG,n_cpus=1,random=None,wanted_task_factor=WANTED_TASK_FACTOR):
        self.identifier = identifier
        self.power = power
        self.n_cpus=n_cpus
        self.wanted_task_factor=wanted_task_factor
        
        if random:
            self.power=int(random.normalvariate(power,power/2.5))
            if self.power>power*3:
                self.power=power*3
            if self.power<power/3:
                self.power=power/3
            if self.power<=0:
                self.power=1

        self.req_period=REQ_PERIOD_MIN
        self.cur_tasks=[]
        self.time_remaining = None 


    def get_n_task_wanted(self):
        return self.n_cpus*self.wanted_task_factor-len(self.cur_tasks)

    def add_task(self, WU, T):
        self.cur_tasks.append((WU,T))

    def set_remaining_time(self, time):
        self.time_remaining = time 

    def get_completed_time(self,end_wu,end_task):
        ctime=0
        
        process_cur_wu = True
        for (WU,T) in self.cur_tasks:            
            if self.time_remaining != None and process_cur_wu:
                ctime+=self.time_remaining
            else:
                ctime+=WU.size/self.power

            if end_wu == WU and T==end_task:
                return ctime

            if process_cur_wu:
                process_cur_wu=False
        return None

    def update_req_period_task(self):
        self.req_period=REQ_PERIOD_MIN

    def update_req_period_notask(self):
        self.req_period = 2*self.req_period  
        if self.req_period > REQ_PERIOD_MAX:
            self.req_period = REQ_PERIOD_MAX

    def __eq__(self,other):
        #return isinstance(other,Worker) and 
        return self.identifier==other.identifier

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "C%s" % (self.identifier)
