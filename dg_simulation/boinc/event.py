
WU_SUBMIT = 0 

CLIENT_ON = 10
CLIENT_OFF = 11

TASK_REQUEST = 20
TASK_RESULT = 21
TASK_WU = 22
TASK_NOWU = 23

WU_VALIDATED = 25

TASK_TIMED_OUT = 31

SPEQULOS_MONITOR = 100


class Event:
    def __init__(self,event_name,client=None,wu=None,task=None,batch_id=None):
        self.name = event_name
        self.client = client
        self.wu = wu
        self.task = task
        self.batch_id = batch_id
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.name==CLIENT_ON:
            return "CLIENT_ON %s"  % self.client
        elif self.name==CLIENT_OFF:
            return "CLIENT_OFF %s" % self.client
        elif self.name==WU_SUBMIT:
            return "WU_SUBMIT %s" % self.wu
        elif self.name==TASK_REQUEST:
            return "TASK_REQUEST %s" % self.client 
        elif self.name==TASK_RESULT:
            return "TASK_RESULT %s %s %s" % (self.client,self.wu,self.task)
        elif self.name==TASK_WU:
            return "TASK_WU %s %s %s" % (self.client,self.wu,self.task)
        elif self.name==TASK_NOWU:
            return "TASK_NOWU %s" % self.client
        elif self.name==WU_VALIDATED:
            return "WU_VALIDATED %s" % self.wu
        elif self.name==TASK_TIMED_OUT:
            return "TASK_TIMED_OUT %s %s %s" % (self.client,self.wu,self.task)
        elif self.name==SPEQULOS_MONITOR:
            return "SPEQULOS_MONITOR" 
        else:
            return str(self.name)

    def __eq__(self,other):
        return self.name==other.name and self.client==other.client and self.wu==other.wu and self.task==other.task
