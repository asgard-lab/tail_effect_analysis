
SIZE_AVG = 1000*1000 #In instructions 
DEFAULT_DELAY_BOUND = 1*24*3600 #Timeout in second
DEFAULT_MIN_QUORUM = 2  #Number of results needed before a task is validated
DEFAULT_TARGET_NRESULT = 3  #Number of task created by default

UNSENT=0
INPROGRESS=1
DONE=2

class WorkUnit:
    def __init__(self,identifier,size=SIZE_AVG,delay=DEFAULT_DELAY_BOUND,quorum=DEFAULT_MIN_QUORUM,target_nresult=DEFAULT_TARGET_NRESULT,batch_id=None):
        self.identifier = identifier
        self.size = size
        self.delay =  delay
        self.quorum = quorum

        self.target_nresult = target_nresult
        self.batch_id = batch_id

        self.tasks = []

        for i in range(self.target_nresult):
            self.tasks.append(Task(i,status=UNSENT))


    def __str__(self):
        return "WU%s" % self.identifier

    def __eq__(self,other):
        #return isinstance(other,Job) and 
        return self.identifier==other.identifier

class Task:
    def __init__(self,identifier,status=UNSENT,assignment=None):
        self.identifier = identifier
        self.assignment = assignment
        self.status = status
 
    def __str__(self):
        return "T%s" % self.identifier

    def __eq__(self,other):
        return self.identifier==other.identifier

   
