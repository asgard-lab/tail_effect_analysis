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
    def add_event(self, time, event):
        i=0
        for (t,e) in self.events:
            if t>time:
                break
            i+=1
        self.events.insert(i,(time,event))            

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
class EventQueueBOINC(EventQueue):

    def __init__(self):
        EventQueue.__init__(self)

    def get_next_completed_time(self, client):
        for (t,e) in self.events:
            if e.client!=None and e.client.identifier==client.identifier:
                if e.name==event.TASK_RESULT:
                    return t
        return None


    def cancel_client_actions(self, client):
        to_remove = []
        for (t,e) in self.events:
            if e.client!=None and e.client.identifier==client.identifier:
                if e.name==event.TASK_RESULT or e.name==event.TASK_REQUEST or e.name==event.TASK_WU:
                    to_remove.append((t,e))
        
        for t in to_remove:
            self.events.remove(t)


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

