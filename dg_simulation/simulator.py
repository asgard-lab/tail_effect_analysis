#!/usr/bin/python

from xw import simulator_xw
from boinc import simulator_boinc
import spequlos

import sys
import getopt

def usage():
    print "Usage: "+sys.argv[0]+" [BOINC|XW] [trace_directory] [BoT_description_file] [options]"
    print """
Options:
    -s <val>
    --start_time=<val>  : simulation start time (default is 0)
    -e <val>
    --end_time=<val>    : simulation end time (default is 10000000000)
    
    --avg_power=<val>   : set node avg power (default is 1000 ops/s)
    --heter_power        : use heterogeneous node power

SpeQuloS options:
    --spequlos          : enable SpeQuloS

    --credits=<val>     : set initial credits (default is 1500)

 When-to-start CW methods
    --thres_compl=<val> : use threshold_completed method at <val> percent of completion (used by default at 90%)
    --thres_assign=<val> : use threshold_assigned method at <val> percent of assignement
    --assign_compl_dist : use assignement_to_completion_distance method

 How-many-to-start CW methods
    --greedy            : use greedy method
    --conservative      : use conservative
    --fixed=<val>       : use a fixed number of CW
    
 What to do with CW methods   
    --naive             : use naive method
    --reschedule        : use reschedule method
    --migrate           : use migrate method
    
"""

start_time=0
end_time=10000000000

avg_power=1000
heter_power=False

use_spequlos = False
sqs = spequlos.SpeQuloS()

try:                                
    opts, args = getopt.gnu_getopt(sys.argv[1:], "s:e:", ["start_time=", "end_time=", "avg_power=", "heter_power", "spequlos", "credits=", "thres_compl=","thres_assign=", "assign_compl_dist", "greedy","conservative","fixed=","naive","reschedule","migrate"])

    for opt,arg in opts:
        if opt in ("-s","--start_time="):
            start_time=int(arg)

        elif opt in ("-e","--end_time="):
            end_time=int(arg)

        elif opt in ("--avg_power="):
            avg_power=int(arg)

        elif opt in ("--heter_power"):
            heter_power=True

        elif opt in ("--spequlos"):
            use_spequlos = True

        elif opt in ("--credits="):
            sqs.init_credits=int(arg)
            
        elif opt in ("--thres_compl="):
            sqs.whentostart_cw_method = spequlos.THRES_C
            sqs.threshold = int(arg)
            if (sqs.threshold < 0 or sqs.threshold>100):
                raise ValueError("Threshold value must be comrised between 0 and 100") 
           
        elif opt in ("--thres_assign="):
            sqs.whentostart_cw_method = spequlos.THRES_A
            sqs.threshold = int(arg)
            if (sqs.threshold < 0 or sqs.threshold>100):
                raise ValueError("Threshold value must be comrised between 0 and 100") 

        elif opt in ("--assign_compl_dist"):
            sqs.whentostart_cw_method = spequlos.ASSIGN_TO_COMPL_TIME

        elif opt in ("--greedy"):
            sqs.howmany_cw_method = spequlos.GREEDY

        elif opt in ("--conservative"):
            sqs.howmany_cw_method = spequlos.CONSERVATIVE

        elif opt in ("--fixed="):
            sqs.howmany_cw_method = spequlos.FIXED
            sqs.howmany_cw = int(arg)

        elif opt in ("--naive"):
            sqs.how_cw_method = spequlos.NAIVE

        elif opt in ("--reschedule"):
            sqs.how_cw_method = spequlos.RESCHED

        elif opt in ("--migrate"):
            sqs.how_cw_method = spequlos.MIGRATE

    dg_type=args[0]
    trace_dir=args[1]
    job_desc_file=args[2]


except Exception as e:
    print e          
    usage()                         
    sys.exit(2)  
    
if not use_spequlos:
    sqs=None

if dg_type == "XW":
    simulator_xw.start_xw_sim(trace_dir,job_desc_file,start_time,end_time,sqs,avg_power,heter_power)

elif dg_type == "BOINC":
    simulator_boinc.start_boinc_sim(trace_dir,job_desc_file,start_time,end_time,sqs,avg_power,heter_power)

