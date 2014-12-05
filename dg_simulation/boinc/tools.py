import sys
import os
import random

from client import *
from event import *
from wu import *

###
### Load Trace Directory and create new BOINC Clients to simulation
###
def load_trace_dir(ev_queue,directory,random_start=True,start_time=0,end_time=10000000000,avg_power=1000,heter_power=False,dir_power=None):
    i=0
    if random_start:
        random.seed(start_time+end_time)
    for (r,d,files) in os.walk(directory):
        for filename in files:            
            if filename[-3:]==".tr":
                i+=1

                if not dir_power == None:
                    with open(dir_power + filename) as power_file:
                        avg_power = int(power_file.readline())
                    power_file.close()

                if heter_power:
                    C=Client(filename[:-3],random=random,power=avg_power)
                else:
                    C=Client(filename[:-3],random=None,power=avg_power)

                f=open(r+filename)                
                for l in f:
                    s=l.split()
                    if int(s[2])==1:
                        start=int(s[0])
                        end=int(s[1])
                         
                        if start < start_time:
                            start=start_time

                        if end > end_time:
                            end=end_time

                        if not (end<=start_time or start >= end_time or start>=end):
                            rs=0
                            if (random_start):
                                rs+=random.randint(0,min(REQ_PERIOD_MAX,end-start-1))

                            ev_queue.add_event(rs+start,Event(CLIENT_ON,C))
                            ev_queue.add_event(end,Event(CLIENT_OFF,C))
                        else:
                            if start>=end_time:
                                break
                f.close()                
                            
###
### Load Job Description File and create new WorkUnit to simulation
###
def load_job_desc_file(ev_queue,job_desc_file,start_time,bot_id=None):
    with open(job_desc_file) as f:
        i=0
        for l in f:
            s=l.split()
            sub_time=int(s[0])
            size=int(s[1])
            WU=WorkUnit(str(i),size=size,batch_id=bot_id)
            ev_queue.add_event(start_time+sub_time,Event(WU_SUBMIT,wu=WU))
            i+=1
    f.close()
