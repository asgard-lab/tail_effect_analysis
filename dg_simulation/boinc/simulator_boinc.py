#!/usr/bin/python

from wu import *
from client import *
from event_queue import *
from event import *
import tools
import spequlos

import sys
import os
import random

DEFAULT_BOT_ID="batch_default"

ONE_RESULT_PER_USER_PER_WU=1

#The boinc scheduler assigns tasks belonging to WUs which have more task already sent to clients (to try to complete a WU ASAP): 0
#The boinc scheduler assigns tasks belonging to WUs which have less task already sent to clients (to try to complete more WUs in less time): 1
#The boinc scheduler assigns tasks randomely (assign one task from a random uncompleted WU): 2
SCHED_ALT=0



def start_boinc_sim(trace_dir,job_desc_file,start_time,end_time,sqs=None,avg_power=1000,heter_power=False,dir_power=None):
    n_wus_remaining=0
    wus=[]
    
    print "--Starting BOINC simulation"
    ev_queue = EventQueueBOINC()
    
    print "--Loading trace--"
    tools.load_trace_dir(ev_queue,trace_dir,start_time=start_time,end_time=end_time,avg_power=avg_power,heter_power=heter_power,dir_power=dir_power)
    
    print "--Loading WUs--"
    tools.load_job_desc_file(ev_queue,job_desc_file,start_time,bot_id=DEFAULT_BOT_ID)
 
    if sqs!=None:
        print "--Starting SpeQuloS--"
        sqs.scheduler_start_qos("SIM_BOINC",DEFAULT_BOT_ID)
        sqs.creditsystem_order(DEFAULT_BOT_ID)
        ev_queue.add_event(start_time,Event(SPEQULOS_MONITOR))
        cloud_wus=[]
        
   
    print "--Processing--"
    
    random.seed(start_time+end_time)
    next_ev = ev_queue.get_next_event()
    while next_ev!=None:
    
        (t,ev) = next_ev
        e = ev.name
        C = ev.client
        WU = ev.wu
        T = ev.task
    
        print t,ev
        if (sqs!=None):
            sqs.info_grabber_boinc(t,ev)

        if e == CLIENT_ON:        
            if len(C.cur_tasks)==0:
                ev_queue.add_event( t, Event(TASK_REQUEST,C) )
            else:
                for (WU,T) in C.cur_tasks:
                    ev_queue.add_event( t+C.get_completed_time(WU,T), Event(TASK_RESULT,C,WU,T) )
    
        elif e == CLIENT_OFF:
            next_completed_time = ev_queue.get_next_completed_time(C) 
            if next_completed_time != None:
                C.set_remaining_time( next_completed_time - t )

            ev_queue.cancel_client_actions( C )
    
        elif e == TASK_REQUEST:        
        
            if sqs==None or (not ("spequloscw" in C.identifier)) or sqs.how_cw_method==spequlos.NAIVE:            
                task_avail=False
                
                for n_task in range(C.get_n_task_wanted()):
                    T=None
                    for w in wus:
                        for ta in w.tasks:
                            if ta.status==UNSENT:
                                WU=w
                                T=ta                    
                            if ONE_RESULT_PER_USER_PER_WU==1 and ta.assignment!=None and ta.assignment==C:
                                #Skipping this WU as a task was already assigned to this client
                                T=None
                                WU=None
                                break
                        if T!=None:
                            break
        
                    if T!=None:
                        task_avail=True
                        T.status=INPROGRESS
                        T.assignment=C
                        ev_queue.add_event( t, Event(TASK_WU,C,WU,T) )
                        ev_queue.add_event( t+WU.delay, Event(TASK_TIMED_OUT,C,WU,T) )
                        
                        if SCHED_ALT==1:
                            wus.remove(WU)
                            wus.append(WU)
                        elif SCHED_ALT==2:
                            wus.remove(WU)
                            wus.insert(random.randint(0,len(wus)),WU)

                if task_avail==False:
                    ev_queue.add_event( t, Event(TASK_NOWU,C) )

            elif sqs.how_cw_method==spequlos.RESCHED:
                task_avail=False

                for n_task in range(C.get_n_task_wanted()):
                    T=None
                    for w in wus:
                        for ta in w.tasks:
                            if ta.status==UNSENT:
                                WU=w
                                T=ta                    
                            if ONE_RESULT_PER_USER_PER_WU==1 and ta.assignment!=None and ta.assignment==C:
                                #Skipping this WU as a task was already assigned to this client
                                T=None
                                WU=None
                                break
                        if T!=None:
                            break
        
                    if T!=None:
                        task_avail=True
                        T.status=INPROGRESS
                        T.assignment=C
                        ev_queue.add_event( t, Event(TASK_WU,C,WU,T) )
                        ev_queue.add_event( t+WU.delay, Event(TASK_TIMED_OUT,C,WU,T) )
                        
                        if SCHED_ALT==1:
                            wus.remove(WU)
                            wus.append(WU)
                        elif SCHED_ALT==2:
                            wus.remove(WU)
                            wus.insert(random.randint(0,len(wus)),WU)

                if task_avail==False:
                    for w in wus:
                        for ta in w.tasks:
                            if ta.status==INPROGRESS:
                                WU=w
                                T=ta                    
                            if ta.assignment!=None and ta.assignment==C:
                                #Skipping this WU as a task was already assigned to this client
                                T=None
                                WU=None
                                break
                        if T!=None:
                            break
        
                    if T!=None:
                        task_avail=True
                        print "SQS: Making special assignment of WU %s T %s to CW %s" % (WU,T,C) 
                        T.assignment=C
                        ev_queue.add_event( t, Event(TASK_WU,C,WU,T) )
                        ev_queue.add_event( t+WU.delay, Event(TASK_TIMED_OUT,C,WU,T) )
                        #Pushing WU at the end of queue for fair scheduling 
                        wus.remove(WU)
                        wus.append(WU)
                    else:
                        ev_queue.add_event( t, Event(TASK_NOWU,C) )

            elif sqs.how_cw_method==spequlos.MIGRATE:
                task_avail=False
                
                for n_task in range(C.get_n_task_wanted()):
                    T=None
                    for w in cloud_wus:
                        for ta in w.tasks:
                            if ta.status==UNSENT:
                                WU=w
                                T=ta                    
                            if ONE_RESULT_PER_USER_PER_WU==1 and ta.assignment!=None and ta.assignment==C:
                                #Skipping this WU as a task was already assigned to this client
                                T=None
                                WU=None
                                break
                        if T!=None:
                            break
        
                    if T!=None:
                        task_avail=True
                        T.status=INPROGRESS
                        T.assignment=C
                        ev_queue.add_event( t, Event(TASK_WU,C,WU,T) )
                        ev_queue.add_event( t+WU.delay, Event(TASK_TIMED_OUT,C,WU,T) )
                        
                        if SCHED_ALT==1:
                            cloud_wus.remove(WU)
                            cloud_wus.append(WU)
                        elif SCHED_ALT==2:
                            cloud_wus.remove(WU)
                            cloud_wus.insert(random.randint(0,len(cloud_wus)),WU)

                if task_avail==False:
                    ev_queue.add_event( t, Event(TASK_NOWU,C) )

    
        elif e == TASK_WU:       
            C.add_task(WU,T)
            ev_queue.add_event( t+C.get_completed_time(WU,T) , Event(TASK_RESULT,C,WU,T) )
            C.update_req_period_task()
    
        elif e == TASK_NOWU:        
            C.update_req_period_notask()
            ev_queue.add_event( t+C.req_period , Event(TASK_REQUEST,C) )
    
        elif e == TASK_RESULT:
            ev_queue.remove_event( Event(TASK_TIMED_OUT,C,WU,T) )
            C.cur_tasks.remove((WU,T))
            T.status=DONE
            
            ok=0
            for i in WU.tasks:
                if i.status==DONE:
                    ok+=1
            
            if ok!=0 and ok>=WU.quorum:            
                if sqs!=None and sqs.how_cw_method==spequlos.MIGRATE:
                    if "spequloscw" in C.identifier:        
                        print "SQS: %s return wu %s task %s" % (C,WU,T)
                        WU_dg=None                  
                        for w in wus:
                            if w.identifier+"c" == WU.identifier:
                                WU_dg=w
                                break

                        if len(WU.tasks)>0 and ( (WU_dg is None) or (len(WU_dg.tasks)>0) ):
                            ev_queue.add_event( t, Event(WU_VALIDATED,wu=WU_dg) )
                        WU.tasks=[]

                    else:
                        WU_c=None                  
                        for w in cloud_wus:
                            if w.identifier == WU.identifier+"c":
                                WU_c=w
                                break

                        if len(WU.tasks)>0 and ( (WU_c is None) or (len(WU_c.tasks)>0) ):
                            ev_queue.add_event( t, Event(WU_VALIDATED,wu=WU) )
                        WU.tasks=[]

                if len(WU.tasks)>0:
                    ev_queue.add_event( t, Event(WU_VALIDATED,wu=WU) )
                WU.tasks=[]
    
            if len(C.cur_tasks)==0:
                ev_queue.add_event( t, Event(TASK_REQUEST,C) )
    
        elif e == WU_VALIDATED:
            n_wus_remaining-=1
           
            if n_wus_remaining==0:
                sys.exit()
    
        elif e == TASK_TIMED_OUT:
            T.assignment=None
            T.status=UNSENT
    
        elif e == WU_SUBMIT:
            wus.append(WU)
    
            n_wus_remaining+=1
 
        elif e == SPEQULOS_MONITOR:
       
            (cw_to_start,cw_to_stop) = sqs.scheduler_monitor_batches()
            cw_to_stop.extend( sqs.scheduler_monitor_cloudworker() )

            for cw_id in cw_to_start: 
                C = Client("spequloscw_%s" % cw_id,power=3*POWER_AVG)
                ev_queue.add_event(t+sqs.cloudworker_get_start_time(random),Event(CLIENT_ON,C))
            for cw_id in cw_to_stop:
                C = Client("spequloscw_%s" % cw_id)
                ev_queue.add_event(t,Event(CLIENT_OFF,C))

            if sqs.how_cw_method==spequlos.MIGRATE:
                if len(cw_to_start)>0:
                    for W in wus:
                        cloud_wu = WorkUnit(identifier=W.identifier+"c",size=W.size,delay=W.delay,quorum=1,target_nresult=1,batch_id=W.batch_id)
                        
                        if len(W.tasks)>0 and (not (cloud_wu in cloud_wus)):
                            cloud_wus.append(cloud_wu)
                            print "SQS: copying wu %s to Cloud DG" % W

            ev_queue.add_event(t+sqs.SCHEDULER_PERIOD, Event(SPEQULOS_MONITOR)) 

   
        next_ev = ev_queue.get_next_event()
    
    
