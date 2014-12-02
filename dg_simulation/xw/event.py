
JOB_SUBMIT = 0 
JOB_COMPLETED = 1

WORKER_ON = 10
WORKER_OFF = 11

WORK_REQUEST = 20
WORK_RESULT = 21
WORK_JOB = 22
WORK_NOJOB = 23

KEEP_ALIVE = 30
WORKER_TIMED_OUT = 31

SPEQULOS_MONITOR = 100

class Event:
    def __init__(self,event_name,worker=None,job=None):
        self.name = event_name
        self.worker = worker
        self.job = job

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.name==WORKER_ON:
            return "WORKER_ON %s"  % self.worker
        elif self.name==WORKER_OFF:
            return "WORKER_OFF %s" % self.worker
        elif self.name==JOB_SUBMIT:
            return "JOB_SUBMIT %s" % self.job
        elif self.name==JOB_COMPLETED:
            return "JOB_COMPLETED %s" % self.job            
        elif self.name==WORK_REQUEST:
            return "WORK_REQUEST %s" % self.worker
        elif self.name==WORK_RESULT:
            return "WORK_RESULT %s %s" % (self.worker,self.job)
        elif self.name==WORK_JOB:
            return "WORK_JOB %s %s" % (self.worker,self.job)
        elif self.name==WORK_NOJOB:
            return "WORK_NOJOB %s" % self.worker
        elif self.name==KEEP_ALIVE:
            return "KEEP_ALIVE %s" % self.worker
        elif self.name==WORKER_TIMED_OUT:
            return "WORKER_TIMED_OUT %s" % self.worker
        elif self.name==SPEQULOS_MONITOR:
            return "SPEQULOS_MONITOR" 
        else:
            return str(self.name)

    def __eq__(self,other):
        return self.name==other.name and self.worker==other.worker and self.job==other.job
