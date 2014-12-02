
POWER_AVG = 1000 #In instructions per second

REQ_PERIOD_MIN=30
REQ_PERIOD_MAX=300

KEEP_ALIVE_PERIOD=60

class Worker:
    def __init__(self,identifier,power=POWER_AVG,random=None,n_cpus=1):
        self.identifier = identifier
        self.power = power
        self.n_cpus=n_cpus
 
        if random:
            self.power=int(random.normalvariate(power,power/2.5))
            if self.power > power*3:
                self.power=power*3
            if self.power < power/3:
                self.power=power/3
            if self.power <= 0:
                self.power=1
       
        self.req_period=REQ_PERIOD_MIN
        self.keepalive_period=KEEP_ALIVE_PERIOD
      

    def get_completion_time(self,n_instruction):
        return n_instruction/self.power

    def update_req_period_job(self):
        self.req_period=REQ_PERIOD_MIN

    def update_req_period_nojob(self):
        self.req_period = 2*self.req_period  
        if self.req_period > REQ_PERIOD_MAX:
            self.req_period = REQ_PERIOD_MAX

    def __eq__(self,other):
        #return isinstance(other,Worker) and 
        return self.identifier==other.identifier

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "W%s" % (self.identifier)
