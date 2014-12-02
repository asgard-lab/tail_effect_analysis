
SIZE_AVG = 1000*1000 #In instructions 

WAITING=0
RUNNING=1
COMPLETED=2

class Job:
    def __init__(self,identifier,size=SIZE_AVG,status=WAITING,assignment=None,bot_id=None):
        self.identifier = identifier
        self.size = size
        self.status = status
        self.assignment = assignment
        self.bot_id = bot_id

    def __str__(self):
        return "J%s" % self.identifier

    def __eq__(self,other):
        #return isinstance(other,Job) and 
        return self.identifier==other.identifier

