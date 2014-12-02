#!/usr/bin/python

import sys

class EventQueue:

    def __init__(self):
        self.events = []

    def add_event(self, time, event):
        start=0
        end=len(self.events)
        while start!=end:
            mid=(start+end)/2
            if time<self.events[mid][0]:
                end=mid
            else:
                start=mid+1
        self.events.insert(start,(time,event))

    """    
    #def add_event(self, time, event):
    #    bisect.insort(self.events,(time,event))
    """

    def remove_event(self, event):
        for (t,e) in self.events:
            if e==event:
                self.events.remove((t,e))   
                return True
        return False

    def get_next_event(self):
        if len(self.events)>0:
            return self.events.pop(0)
        else:
            return None

    def print_queue(self):
        for (t,e) in self.events:
            print t,e

import event
class EventQueueXW(EventQueue):

    def __init__(self):
        EventQueue.__init__(self)
        self.workertimedout_by_worker = {}

    def cancel_worker_actions(self, worker):
        to_remove = []
        for (t,e) in self.events:
            if e.worker!=None and e.worker.identifier==worker.identifier:
                if e.name==event.WORK_RESULT or e.name==event.KEEP_ALIVE or e.name==event.WORK_REQUEST or e.name==event.WORK_JOB:
                    to_remove.append((t,e))
        
        for t in to_remove:
            self.events.remove(t)

    def update_worker_timedout(self,time,ev):
        W=ev.worker  
        if W.identifier in self.workertimedout_by_worker:
            to = self.workertimedout_by_worker[W.identifier]
            if time<to[0]:
                self.events.remove(to)
        self.workertimedout_by_worker[ev.worker.identifier]=((time,ev))
        self.add_event(time,ev)
 
    def get_first_event_time(self):
        if len(self.events)>0:
            (t,e) = self.events[0]
            return t
        else:
            return 0
        
       





if __name__=="__main__":
    import random
    q = EventQueue()

    q.add_event(2,1)
    ev = q.get_next_event()
    MAX_EV=10000
    n_ev=0
    while ev!=None:
        if (n_ev<MAX_EV):
            q.add_event(random.randint(0,MAX_EV),1)
            n_ev+=1
        if (n_ev<MAX_EV):
            q.add_event(random.randint(0,MAX_EV),2)
            n_ev+=1
        if (n_ev%10000==0):
            print n_ev
        ev = q.get_next_event()

