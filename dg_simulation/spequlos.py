#!/usr/bin/python

import time
import xw
import boinc
from xw import event as XWEV
from boinc import event as BEV
import random

THRES_C=10
THRES_A=20
ASSIGN_TO_COMPL_TIME=30

GREEDY=50
CONSERVATIVE=51
FIXED=52

NAIVE=100
RESCHED=101
MIGRATE=102

def spequlos_log(s):
    print "SQS: "+s

class SpeQuloS:

    SCHEDULER_PERIOD = 60 * 4
    CLOUDWORKER_STARTTIME = 150 
    COST_EACH_TURN=1 #SCHEDULER_PERIOD*15/3600



    def __init__(self,n_cloudworkers=10000,dg_ids=["SIM_XW","SIM_BOINC"],whentostart_cw_method=THRES_C,threshold=90,howmany_cw_method=CONSERVATIVE,howmany_cw=10,how_cw_method=NAIVE,init_credits=1500):
        self.info_bot_db={}             # bot_id -> [(t,n_completed,n_uncompleted,n_assigned)]
        self.scheduler_bot=[]  
        self.info_worker_db={}          # dg_id -> [(t,n_worker,n_cloudworker)]
        self.scheduler_dg=[]
        for dg_id in dg_ids:
            self.scheduler_dg.append(dg_id)
        self.scheduler_cloudworkers=[]  # [[cw_id,bot_id]]
        for i in range(n_cloudworkers):
            self.scheduler_cloudworkers.append((str(i),None))
        self.creditsystem_bill={}       #  bot_id -> (credit_available)
       
        self.whentostart_cw_method=whentostart_cw_method
        self.threshold=threshold
        
        self.howmany_cw_method=howmany_cw_method
        self.howmany_cw = howmany_cw
        
        self.how_cw_method=how_cw_method
        
        self.init_credits=init_credits
        
        self.info_cw_activity={}
        self.cw_started=False
        self.info_task_seen = {}

        
    def dump(self):
        print self.whentostart_cw_method,self.threshold,self.howmany_cw_method
    

    def scheduler_start_qos(self,dg_id,bot_id=None):
        if not dg_id in self.scheduler_dg:
            spequlos_log("Unknown dg_id %s" % dg_id)
        if bot_id is None:
            bot_id="batch-%s" % time.time()
        self.scheduler_bot.append(bot_id)
        spequlos_log("SpeQuloS BoT managment started for %s @ %s" % (bot_id,dg_id))

    def creditsystem_order(self,bot_id,credits=None):
        if credits is None:
            credits=self.init_credits
            
        if not bot_id in self.creditsystem_bill:
            self.creditsystem_bill[bot_id]=0
        self.creditsystem_bill[bot_id]+=credits
        spequlos_log("Ordering %d credits for %s" % (credits,bot_id))

    def creditsystem_get_credits(self,bot_id):
        if not bot_id in self.creditsystem_bill:
            return None
        else:
            return self.creditsystem_bill[bot_id]

    def info_grabber_xw(self,t, e, dg_name="SIM_XW"):
        if e.name == XWEV.JOB_SUBMIT:
            bot_id = e.job.bot_id
            if bot_id in self.scheduler_bot:
                if bot_id in self.info_bot_db:
                    (pred_t,pred_nc,pred_nuc,pred_na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
                else: 
                    (pred_t,pred_nc,pred_nuc,pred_na) = (0,0,0,0)
                    self.info_bot_db[bot_id]=[]

                self.info_bot_db[bot_id].append((t,pred_nc,pred_nuc+1,pred_na))
                spequlos_log("Adding one uncompleted job to Info DB")

        if e.name == XWEV.JOB_COMPLETED:
            bot_id = e.job.bot_id
            if bot_id in self.scheduler_bot:
                if bot_id in self.info_bot_db:
                    (pred_t,pred_nc,pred_nuc,pred_na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
                else: 
                    (pred_t,pred_nc,pred_nuc,pred_na) = (0,0,0,0)
                    self.info_bot_db[bot_id]=[]
            
                self.info_bot_db[bot_id].append((t,pred_nc+1,pred_nuc-1,pred_na))
                spequlos_log("Adding one completed job to Info DB")           

        if e.name == XWEV.WORK_JOB:
            bot_id = e.job.bot_id
            worker_id =e.worker.identifier
            job_id = e.job.identifier
            if job_id[-1]=="c":
                job_id=job_id[:-1]

            if bot_id in self.scheduler_bot:
                                 
                if not (job_id in self.info_task_seen):
                    self.info_task_seen[job_id]=0
                self.info_task_seen[job_id]+=1
                
                if self.info_task_seen[job_id]==1:
                    if bot_id in self.info_bot_db:
                        (pred_t,pred_nc,pred_nuc,pred_na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
                    else: 
                        (pred_t,pred_nc,pred_nuc,pred_na) = (0,0,0,0)
                        self.info_bot_db[bot_id]=[]

                    self.info_bot_db[bot_id].append((t,pred_nc,pred_nuc,pred_na+1))
                    spequlos_log("Adding one assigned job to Info DB")
     
                if "spequloscw" in worker_id:
                    cw_id = worker_id.replace("spequloscw_","")
                    if not cw_id in self.info_cw_activity:
                        self.info_cw_activity[cw_id]=0
                    self.info_cw_activity[cw_id]+=1
                    spequlos_log("Adding one job to CW activity %s" % cw_id)

        if e.name == XWEV.WORK_RESULT:
            worker_id =e.worker.identifier            
     
            if "spequloscw" in worker_id:
                cw_id = worker_id.replace("spequloscw_","")
                self.info_cw_activity[cw_id]-=1
                spequlos_log("Removing one job to CW activity %s" % cw_id)     
 
        if e.name == XWEV.WORK_NOJOB:
            worker_id =e.worker.identifier
            if "spequloscw" in worker_id:
                    cw_id = worker_id.replace("spequloscw_","")
                    if not cw_id in self.info_cw_activity:
                        self.info_cw_activity[cw_id]=0
                        spequlos_log("Setting CW activity %s to zero" % cw_id)
                    
                   
        if e.name == XWEV.WORKER_ON:
           if dg_name in self.info_worker_db:
               (pred_t,pred_nw,pred_ncw) = self.info_worker_db[dg_name][len(self.info_worker_db[dg_name])-1]
           else:
               (pred_t,pred_nw,pred_ncw) = (0,0,0)
               self.info_worker_db[dg_name]=[]

           if isinstance(e.worker.identifier,str) and ("spequloscw" in e.worker.identifier):
               self.info_worker_db[dg_name].append((t,pred_nw+1,pred_ncw+1))
               spequlos_log("Adding one cloud worker to Info DB")
           else:
               self.info_worker_db[dg_name].append((t,pred_nw+1,pred_ncw))
               spequlos_log("Adding one worker to Info DB")

        if e.name == XWEV.WORKER_OFF:
           if dg_name in self.info_worker_db:
               (pred_t,pred_nw,pred_ncw) = self.info_worker_db[dg_name][len(self.info_worker_db[dg_name])-1]
           else:
               (pred_t,pred_nw,pred_ncw) = (0,0,0)
               self.info_worker_db[dg_name]=[]

           if isinstance(e.worker.identifier,str) and ("spequloscw" in e.worker.identifier):
               self.info_worker_db[dg_name].append((t,pred_nw-1,pred_ncw-1))
               spequlos_log("Removing one cloud worker to Info DB")
           else:
               self.info_worker_db[dg_name].append((t,pred_nw-1,pred_ncw))
               spequlos_log("Removing one worker to Info DB")

    def info_grabber_boinc(self,t, e, dg_name="SIM_BOINC"):
        if e.name == BEV.WU_SUBMIT:
            bot_id = e.wu.batch_id
            if bot_id in self.scheduler_bot:
                if bot_id in self.info_bot_db:
                    (pred_t,pred_nc,pred_nuc,pred_na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
                else: 
                    (pred_t,pred_nc,pred_nuc,pred_na) = (0,0,0,0)
                    self.info_bot_db[bot_id]=[]

                self.info_bot_db[bot_id].append((t,pred_nc,pred_nuc+1,pred_na))
                spequlos_log("Adding one uncompleted job to Info DB")
            
        if e.name == BEV.WU_VALIDATED:
            bot_id = e.wu.batch_id
            if bot_id in self.scheduler_bot:
                if bot_id in self.info_bot_db:
                    (pred_t,pred_nc,pred_nuc,pred_na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
                else: 
                    (pred_t,pred_nc,pred_nuc,pred_na) = (0,0,0,0)
                    self.info_bot_db[bot_id]=[]

                self.info_bot_db[bot_id].append((t,pred_nc+1,pred_nuc-1,pred_na))
                spequlos_log("Adding one completed job to Info DB")
             
        if e.name == BEV.TASK_RESULT:
            client_id = e.client.identifier
            if "spequloscw" in client_id:
                cw_id = client_id.replace("spequloscw_","")
                self.info_cw_activity[cw_id]-=1
                spequlos_log("Removing one job to CW activity %s" % cw_id)
              
        if e.name == BEV.TASK_NOWU:
            client_id = e.client.identifier
            if "spequloscw" in client_id:
                cw_id = client_id.replace("spequloscw_","")
                if not cw_id in self.info_cw_activity:
                    self.info_cw_activity[cw_id]=0
                    spequlos_log("Setting CW activity to zero %s" % cw_id)
            
        if e.name == BEV.TASK_WU:
            bot_id = e.wu.batch_id
            wu_id = e.wu.identifier
            if wu_id[-1]=="c":
                wu_id=wu_id[:-1]
            
            client_id = e.client.identifier
            if bot_id in self.scheduler_bot:
                if not (wu_id in self.info_task_seen):
                    self.info_task_seen[wu_id]=0
                self.info_task_seen[wu_id]+=1
                if self.info_task_seen[wu_id]==1: 
                    #self.info_task_seen[wu_id]+=100
                    if bot_id in self.info_bot_db:
                        (pred_t,pred_nc,pred_nuc,pred_na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
                    else: 
                        (pred_t,pred_nc,pred_nuc,pred_na) = (0,0,0,0)
                        self.info_bot_db[bot_id]=[]

                    self.info_bot_db[bot_id].append((t,pred_nc,pred_nuc,pred_na+1))
                    spequlos_log("Adding one assigned job to Info DB")
  
                if "spequloscw" in client_id:
                    cw_id = client_id.replace("spequloscw_","")
                    if not cw_id in self.info_cw_activity:
                        self.info_cw_activity[cw_id]=0
                    self.info_cw_activity[cw_id]+=1
                    spequlos_log("Adding one job to CW activity %s" % cw_id)
            
        if e.name == BEV.CLIENT_ON:
           C = e.client.identifier
           if dg_name in self.info_worker_db:
               (pred_t,pred_nw,pred_ncw) = self.info_worker_db[dg_name][len(self.info_worker_db[dg_name])-1]
           else:
               (pred_t,pred_nw,pred_ncw) = (0,0,0)
               self.info_worker_db[dg_name]=[]

           if isinstance(C,str) and ("spequloscw" in C):
               self.info_worker_db[dg_name].append((t,pred_nw+1,pred_ncw+1))
               spequlos_log("Adding one cloud worker to Info DB")
           else:
               self.info_worker_db[dg_name].append((t,pred_nw+1,pred_ncw))
               spequlos_log("Adding one worker to Info DB")

        if e.name == BEV.CLIENT_OFF:
           C = e.client.identifier
           if dg_name in self.info_worker_db:
               (pred_t,pred_nw,pred_ncw) = self.info_worker_db[dg_name][len(self.info_worker_db[dg_name])-1]
           else:
               (pred_t,pred_nw,pred_ncw) = (0,0,0)
               self.info_worker_db[dg_name]=[]

           if isinstance(C,str) and ("spequloscw" in C):
               self.info_worker_db[dg_name].append((t,pred_nw-1,pred_ncw-1))
               spequlos_log("Removing one cloud worker to Info DB")
           else:
               self.info_worker_db[dg_name].append((t,pred_nw-1,pred_ncw))
               spequlos_log("Removing one worker to Info DB")


    def oracle_get_uncompleted_jobs(self,bot_id):
        if not bot_id in self.scheduler_bot:
            return None
        elif (not bot_id in self.info_bot_db) or len(self.info_bot_db[bot_id])==0:
            return 0
        else:
            (t,nc,nuc,na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
            return nuc 

    def oracle_get_completion(self,bot_id):
        if not bot_id in self.scheduler_bot:
            return None
        elif (not bot_id in self.info_bot_db) or len(self.info_bot_db[bot_id])==0:
            return 0.0
        else:
            (t,nc,nuc,na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
            return nc/float(nc+nuc) 

    def oracle_get_assigned(self,bot_id):
        if not bot_id in self.scheduler_bot:
            return None
        elif (not bot_id in self.info_bot_db) or len(self.info_bot_db[bot_id])==0:
            return 0.0
        else:
            (t,nc,nuc,na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
            return na/float(nc+nuc) 

    def oracle_get_estimated_remaining_time(self,bot_id):
        if not bot_id in self.scheduler_bot:
            return None
        elif (not bot_id in self.info_bot_db) or len(self.info_bot_db[bot_id])==0:
            return None 
        else:
            (t,nc,nuc,na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
            (t_0,nc_0,nuc_0,na_0) = self.info_bot_db[bot_id][0]
            elapsed_time=t-t_0
            njobs=nc+nuc
            percent_completed=nc/float(njobs)
            if percent_completed>0:
                est=elapsed_time/percent_completed-elapsed_time
                return est 
            else:
                return None

    def oracle_get_assigned_to_compl_time(self,bot_id):
        if not bot_id in self.scheduler_bot:
            return None
        elif (not bot_id in self.info_bot_db) or len(self.info_bot_db[bot_id])==0:
            return (None,None)
        else:
            max_actime_50_first = None 
            max_actime_50_last = None
            atime = {}
            ctime = {}
            ac_time = {}
            for (t,nc,nuc,na) in self.info_bot_db[bot_id]:
                if not na in atime:
                    atime[na]=t
                if not nc in ctime:
                    ctime[nc]=t
                n_jobs=nc+nuc
            for n in ctime:
                if (n in ctime) and (n in atime):
                    ac_time[n]=ctime[n]-atime[n]
            for n in sorted(ac_time.keys()):
                if ( n/float(n_jobs) <= 0.5 ):
                  if max_actime_50_first == None or max_actime_50_first<ac_time[n]:
                    max_actime_50_first=ac_time[n]
                else:
                  if max_actime_50_last == None or max_actime_50_last<ac_time[n]:
                    max_actime_50_last=ac_time[n]
            return (max_actime_50_first,max_actime_50_last)

    def oracle_calculate_cloud_benefit(self,bot_id):
        if not bot_id in self.scheduler_bot:
            return None
        elif (not bot_id in self.info_bot_db) or len(self.info_bot_db[bot_id])==0:
            return 0.0
        else:
            (t,nc,nuc,na) = self.info_bot_db[bot_id][len(self.info_bot_db[bot_id])-1]
            (t_0,nc_0,nuc_0,na_0) = self.info_bot_db[bot_id][0]
            elapsed_time=t-t_0
            njobs=nc+nuc
            percent_completed=nc/float(njobs)
            t_90v=0.9*elapsed_time/percent_completed
            t_10v=0.1*elapsed_time/percent_completed
            t_10c=t_10v/3
            prediction = (t_90v+t_10v,2*t_90v,t_90v+t_10c,njobs/10)
            return (prediction[1]/prediction[2],prediction[3])


    def oracle_should_start_cw(self,bot_id):
        if self.whentostart_cw_method == THRES_C:
            c=self.oracle_get_completion(bot_id)
            spequlos_log("Completion is %s" % c)
            return c >= self.threshold/100.0
        elif self.whentostart_cw_method == THRES_A:
            a=self.oracle_get_assigned(bot_id)
            spequlos_log("Assignment is %f" % a)
            return a >= self.threshold/100.0
        elif self.whentostart_cw_method == ASSIGN_TO_COMPL_TIME:
            c=self.oracle_get_completion(bot_id)
            (first,last) = self.oracle_get_assigned_to_compl_time(bot_id)
            spequlos_log("Completion is %s and assignment to completion distance is %s %s (first half second half)" % (c,first,last))
            if first!=None and last!=None:
                return (c>=0.75) and (last > first*1.5)
            else:
                return False

    def scheduler_howmany_cw_to_start(self,bot_id):
        cw_to_start=[]
        if self.howmany_cw_method == GREEDY:
            spequlos_log("Using method GREEDY to start cw")
            if self.cw_started:
                spequlos_log("CW was already started for this batch, doing nothing")
            else:
                started=0
                to_start=self.creditsystem_get_credits(bot_id)/15
                for (cw_id,cw_bot_id) in self.scheduler_cloudworkers:
                    if started>=to_start:
                        break #TODO remove later?
                    if cw_bot_id == None:
                        spequlos_log("Starting CW %s" % cw_id)
                        self.scheduler_cloudworkers[self.scheduler_cloudworkers.index((cw_id,cw_bot_id))] = (cw_id,bot_id)
                        cw_to_start.append(cw_id)
                        started+=1
                        self.cw_started=True
        elif self.howmany_cw_method == CONSERVATIVE:
            spequlos_log("Using method CONSERVATIVE to start cw")
            if self.cw_started:
                spequlos_log("CW was already started for this batch, doing nothing")
            else:
                started=0
                est=self.oracle_get_estimated_remaining_time(bot_id)
                if est>0:
                  spequlos_log("Estimated completion in %s" % str(est/3600))
                  if est<3600:
                      est=3600
                  to_start=int(self.creditsystem_get_credits(bot_id)/15/(est/3600))                  
                  for (cw_id,cw_bot_id) in self.scheduler_cloudworkers:
                    if started>=to_start:
                        break #TODO remove later?
                    if cw_bot_id == None:
                        spequlos_log("Starting CW %s" % cw_id)
                        self.scheduler_cloudworkers[self.scheduler_cloudworkers.index((cw_id,cw_bot_id))] = (cw_id,bot_id)
                        cw_to_start.append(cw_id)
                        started+=1
                        self.cw_started=True
                else:
                  spequlos_log("Cannot compute estimated completion")
        elif self.howmany_cw_method == FIXED:
            spequlos_log("Using method FIXED to start cw")
            if self.cw_started:
                spequlos_log("CW was already started for this batch, doing nothing")
            else:
                started=0
                to_start=self.howmany_cw
                for (cw_id,cw_bot_id) in self.scheduler_cloudworkers:
                    if started>=to_start:
                        break #TODO remove later?
                    if cw_bot_id == None:
                        spequlos_log("Starting CW %s" % cw_id)
                        self.scheduler_cloudworkers[self.scheduler_cloudworkers.index((cw_id,cw_bot_id))] = (cw_id,bot_id)
                        cw_to_start.append(cw_id)
                        started+=1
                        self.cw_started=True

        return cw_to_start 


    def scheduler_monitor_batches(self):
        cw_to_start=[]
        cw_to_stop=[]
        for bot_id in self.scheduler_bot:
            spequlos_log("Checking bot %s" % bot_id)
            completion = self.oracle_get_completion(bot_id)
            if completion>=1:
                spequlos_log("Completion too high, removing bot")
                self.scheduler_bot.remove(bot_id)
                for (cw_id,cw_bot_id) in self.scheduler_cloudworkers:
                    if bot_id == cw_bot_id:
                        spequlos_log("Stopping CW %s" % cw_id)
                        self.scheduler_cloudworkers[self.scheduler_cloudworkers.index((cw_id,cw_bot_id))] = (cw_id,None)
                        cw_to_stop.append(cw_id)
            else:
                credits = self.creditsystem_get_credits(bot_id)
                if credits<=0:
                    spequlos_log("No credit ordered for this bot")
                else:
                    spequlos_log("%s credits ordered for this bot" % credits)
                    if self.oracle_should_start_cw(bot_id):
                        spequlos_log("Scheduler should start CW")
                        cw_to_start=self.scheduler_howmany_cw_to_start(bot_id)
                    else:
                        spequlos_log("Scheduler should not start CW")
        return cw_to_start,cw_to_stop


    def scheduler_monitor_cloudworker(self):
        cw_to_stop=[]
        for (cw_id,cw_bot_id) in self.scheduler_cloudworkers:
            if cw_bot_id!=None:
                spequlos_log("Checking CW %s" % cw_id)
                completion = self.oracle_get_completion(cw_bot_id)
                if completion>=1:
                    spequlos_log("BoT finnished, stopping CW")
                    self.scheduler_cloudworkers[self.scheduler_cloudworkers.index((cw_id,cw_bot_id))] = (cw_id,None)
                    cw_to_stop.append(cw_id)
                if (cw_id in self.info_cw_activity) and (self.info_cw_activity[cw_id]==0):
                    spequlos_log("CW is unused, stopping it")
                    self.scheduler_cloudworkers[self.scheduler_cloudworkers.index((cw_id,cw_bot_id))] = (cw_id,None)
                    cw_to_stop.append(cw_id)
                else:
                    if self.creditsystem_bill[cw_bot_id]>=self.COST_EACH_TURN:
                        self.creditsystem_bill[cw_bot_id]-=self.COST_EACH_TURN
                    else:
                        spequlos_log("No more credits, stopping CW")
                        self.scheduler_cloudworkers[self.scheduler_cloudworkers.index((cw_id,cw_bot_id))] = (cw_id,None)
                        cw_to_stop.append(cw_id)
                spequlos_log("%s credits remaining" % self.creditsystem_get_credits(cw_bot_id))
        return cw_to_stop


    def cloudworker_get_start_time(self,random=None):
        if random:
            start_time = int(random.normalvariate(self.CLOUDWORKER_STARTTIME,self.CLOUDWORKER_STARTTIME/10.0)) 
            if start_time > self.CLOUDWORKER_STARTTIME*2:
                start_time=self.CLOUDWORKER_STARTTIME*2
            if start_time < self.CLOUDWORKER_STARTTIME/2:
                start_time=self.CLOUDWORKER_STARTTIME/2
            if start_time <= 0:
                start_time=1      
        else:
            start_time=self.CLOUDWORKER_STARTTIME
        return start_time

if __name__=="__main__":
    from xw.worker import *
    from xw.job import *
    sqs = SpeQuloS()
    sqs.scheduler_start_qos("SIM_XW","toto")
    sqs.creditsystem_order("toto",1)
    t=0
    for i in range(10):
        sqs.info_grabber_xw(t,XWEV.Event(XWEV.WORKER_ON,worker=Worker(str(i))))
        t+=60
    for i in range(100):
        sqs.info_grabber_xw(t,XWEV.Event(XWEV.JOB_SUBMIT,job=Job(i,bot_id="toto")))
        t+=60
    for i in range(50):
        sqs.info_grabber_xw(t,XWEV.Event(XWEV.WORK_JOB,job=Job(i,bot_id="toto"),worker=Worker(str(i))))
        t+=60
    for i in range(50):
        sqs.info_grabber_xw(t,XWEV.Event(XWEV.WORK_RESULT,job=Job(i,bot_id="toto"),worker=Worker(str(i))))
        print sqs.scheduler_monitor_batches()
        print sqs.scheduler_monitor_cloudworker()
        t+=60
    for i in range(50):
        sqs.info_grabber_xw(t,XWEV.Event(XWEV.WORK_JOB,job=Job(i,bot_id="toto"),worker=Worker(str(i))))
        t+=60
    for i in range(50):
        sqs.info_grabber_xw(t,XWEV.Event(XWEV.WORK_RESULT,job=Job(i,bot_id="toto"),worker=Worker(str(i))))
        print sqs.scheduler_monitor_batches()
        print sqs.scheduler_monitor_cloudworker()
        t+=60
 
