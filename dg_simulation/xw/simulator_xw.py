#!/usr/bin/python

from job import *
from worker import *
from event_queue import *
from event import *
import tools
import spequlos

import random
import sys
import os


DEFAULT_BOT_ID="batch_default"


def start_xw_sim(trace_dir,job_desc_file,start_time,end_time,sqs=None,avg_power=1000,heter_power=False):
    n_jobs_remaining=0
    jobs=[]
    
    print "--Starting XW simulation"
    WORKER_TIMEOUT=60*15
    ev_queue = EventQueueXW()
    
    print "--Loading trace--"
    tools.load_trace_dir(ev_queue,trace_dir,start_time=start_time,end_time=end_time,avg_power=avg_power,heter_power=heter_power)

    print "--Loading jobs--"
    tools.load_job_desc_file(ev_queue,job_desc_file,start_time,bot_id=DEFAULT_BOT_ID)
  
    #print ev_queue.get_first_event_time()

    if sqs!=None:
        print "--Starting SpeQuloS--"
        sqs.scheduler_start_qos("SIM_XW",DEFAULT_BOT_ID)
        sqs.creditsystem_order(DEFAULT_BOT_ID)
        ev_queue.add_event(start_time,Event(SPEQULOS_MONITOR))
        cloud_jobs=[]

    print "--Processing--"
    
    random.seed(start_time+end_time)    
    next_ev = ev_queue.get_next_event()
    while next_ev!=None:
    
        (t,ev) = next_ev
        e = ev.name
        W = ev.worker
        J = ev.job
   
        if (sqs!=None):
            sqs.info_grabber_xw(t,ev)
        print t,ev
    
        if e == WORKER_ON:
            ev_queue.add_event( t, Event(KEEP_ALIVE,W) )
            for i in range(W.n_cpus):
                ev_queue.add_event( t, Event(WORK_REQUEST,W) )
    
            for J in jobs:
                if (not (J.assignment is None)) and J.assignment==W and J.status==RUNNING:
                    J.assignment=None
                    J.status=WAITING
    
        elif e == WORKER_OFF:
            ev_queue.cancel_worker_actions( W )
    
        elif e == WORK_REQUEST:        

            if sqs==None or (not ("spequloscw" in W.identifier)) or sqs.how_cw_method==spequlos.NAIVE:
                J=None
                for c in jobs:
                    if c.status==WAITING:
                        J=c
                        break
        
                if J!=None:
                    J.status=RUNNING       
                    J.assignment=W                        
                    ev_queue.add_event( t, Event(WORK_JOB,W,J) )                
                else:
                    ev_queue.add_event( t, Event(WORK_NOJOB,W) )
            
            elif sqs.how_cw_method==spequlos.RESCHED:
                J=None
                for c in jobs:
                    if c.status==WAITING:
                        J=c
                        break
        
                if J!=None:
                    ev_queue.add_event( t, Event(WORK_JOB,W,J) )
                else:
                    for c in jobs:
                        if c.status==RUNNING and (not ("spequloscw" in c.assignment.identifier)):
                            J=c
                            break
                    if J!=None:
                        print "SQS: RESCHED Making special assignment of job %s to worker %s" % (J,W)
                        J.status=RUNNING       
                        J.assignment=W                        
                        ev_queue.add_event( t, Event(WORK_JOB,W,J) )
                    else:
                        ev_queue.add_event( t, Event(WORK_NOJOB,W) )
            
            elif sqs.how_cw_method==spequlos.MIGRATE:
                J=None
                for c in cloud_jobs:
                    if c.status==WAITING:
                        J=c
                        break
        
                if J!=None:
                    print "SQS: MIGRATE Making special assignment of job %s to worker %s" % (J,W)                            
                    J.status=RUNNING       
                    J.assignment=W
                    ev_queue.add_event( t, Event(WORK_JOB,W,J) )
                else:
                    ev_queue.add_event( t, Event(WORK_NOJOB,W) )

    
        elif e == WORK_JOB:        

            ev_queue.add_event( t+W.get_completion_time(J.size) , Event(WORK_RESULT,W,J) )
            ev_queue.add_event( t+W.get_completion_time(J.size) , Event(WORK_REQUEST,W) )
            W.update_req_period_job()
    
        elif e == WORK_NOJOB:        
            W.update_req_period_nojob()
            ev_queue.add_event( t+W.req_period , Event(WORK_REQUEST,W) )
    
        elif e == WORK_RESULT:
            if sqs!=None and sqs.how_cw_method==spequlos.MIGRATE:
                if "spequloscw" in W.identifier:
                    print "SQS: %s return job %s" % (W,J)
                    J_dg=None
                    for j in jobs:
                        if j.identifier+"c" == J.identifier:
                            J_dg=j
                            break

                    if J.status!=COMPLETED and ( (J_dg is None) or (J_dg.status!=COMPLETED) ):            
                        ev_queue.add_event( t, Event(JOB_COMPLETED, job=J_dg) )
                    J.status=COMPLETED

                else:
                    J_c=None
                    for j in cloud_jobs:
                        if j.identifier == J.identifier+"c":
                            J_c=j
                            break

                    if J.status!=COMPLETED and ( (J_c is None) or (J_c.status!=COMPLETED) ):            
                        ev_queue.add_event( t, Event(JOB_COMPLETED, job=J) )
                    J.status=COMPLETED

            else: 
                if J.status!=COMPLETED:            
                    ev_queue.add_event( t, Event(JOB_COMPLETED, job=J) )                        
                J.status=COMPLETED

        elif e == JOB_COMPLETED:
            n_jobs_remaining-=1
            if n_jobs_remaining==0:
                sys.exit()                
            
    
        elif e == KEEP_ALIVE:
            ev_queue.add_event( t+W.keepalive_period, Event(KEEP_ALIVE, W) )
            ev_queue.remove_event( Event(WORKER_TIMED_OUT,W) )
            ev_queue.add_event( t+WORKER_TIMEOUT, Event(WORKER_TIMED_OUT, W) )
    
        elif e == WORKER_TIMED_OUT:
            for J in jobs:
                if (not (J.assignment is None)) and J.assignment==W and J.status==RUNNING:
                    J.assignment=None
                    J.status=WAITING
    
        elif e == JOB_SUBMIT:
            J.assignment=None
            J.status=WAITING
            jobs.append(J)
    
            n_jobs_remaining+=1     
     
        elif e == SPEQULOS_MONITOR:

            (cw_to_start,cw_to_stop) = sqs.scheduler_monitor_batches()
            cw_to_stop.extend( sqs.scheduler_monitor_cloudworker() )
            
            for cw_id in cw_to_start: 
                W = Worker("spequloscw_%s" % cw_id,power=3*POWER_AVG)
                ev_queue.add_event(t+sqs.cloudworker_get_start_time(random),Event(WORKER_ON,W))
            for cw_id in cw_to_stop:
                W = Worker("spequloscw_%s" % cw_id)
                ev_queue.add_event(t,Event(WORKER_OFF,W))

            if sqs.how_cw_method==spequlos.MIGRATE:
                if len(cw_to_start)>0:
                    for J in jobs:
                        cloud_job = Job(identifier=J.identifier+"c",size=J.size,bot_id=J.bot_id)
                        
                        if J.status!=COMPLETED and (not (cloud_job in cloud_jobs)):
                            cloud_jobs.append(cloud_job)
                            print "SQS: copying job %s to Cloud DG" % J
              

            ev_queue.add_event(t+sqs.SCHEDULER_PERIOD, Event(SPEQULOS_MONITOR)) 

        next_ev = ev_queue.get_next_event()
