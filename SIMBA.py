'''
SIMBA - Slurm Itteration Manager for Batch Inputs

A handy wrapper for using pandas dataframes and .dat files to make tracking / organizing sbatch files

HM 23
'''

import pandas as pd
import time
import numpy as np
import os
from argparse import ArgumentParser
import os


#----------------------
#Default variables
_def_tab_url = "./SIMBA_jobstatus.dat"
_load_sleeptime = 0.5
_load_maxits = 20
#----------------------
def _timef():
    t= time.localtime()
    wkdays = ["Mon","Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    return "%02i:%02i %s %02i/%02i" %(t.tm_hour, t.tm_min, wkdays[t.tm_wday], t.tm_mday, t.tm_mon)
    
def _safetyload(func):
    '''
    Decorate to make sure we never open a file that's already open
    and to check it out from being edited while we're accessing it

    Decorated function must have target file url named 'table_url'
    '''

    argnames = func.__code__.co_varnames[:func.__code__.co_argcount]
    fname = func.__name__

    #--------------------
    def wrapped_func(*args, **kwargs):

        #Get input 'table_url'
        table_url=_def_tab_url
        for argname, arg in zip(argnames, args):
            if argname=='table_url': table_url=arg
            break

        #Check if file exists
        if not os.path.isfile(table_url):
            if fname=="make":
                #If file doesn't exit, feel free to trigger make...
                return( func(*args, **kwargs) )
            else:
                #But don't try to edit a non existant file otherwise
                raise Exception("Attempted to edit non-existant file %s in %s" %(table_url, fname))
        else:
            #Try to rename the file to see if we have access
            its=0
            while its<_load_maxits:
                its+=1
                try:
                    os.rename(table_url,table_url+"_")
                    os.rename(table_url+"_",table_url)
                    file = open(table_url)
                    print("good login of %s in %s" %(table_url, fname))
                    break
                except:
                    print("bad login of %s attempt %i in %s" %(table_url, its, fname))
                    time.sleep(_load_sleeptime)
        
            #Execute main function
            return( func(*args, **kwargs) )

        #Close the file
        file.close()
    #--------------------
    return wrapped_func

#----------------------
@_safetyload
def make(args=None, table_url=_def_tab_url):
    '''
    Makes a job status table /w args. 
    '''
    print("Making table at %s" %table_url)
    #MISSINGNO: Warning for over-writing existing

    #------------------
    #MISSINGNO - different input types
    if args!=None:
        Njobs = len(args[list(args.keys())[0]])

    #------------------
    #Make and save dataframe
    df = pd.DataFrame({"finished":[False]*Njobs, "start_time":[" "*len(_timef())]*Njobs, "finish_time":[" "*len(_timef())]*Njobs} | args)
    df.to_csv(table_url, sep="\t")

#----------------------

@_safetyload
def start(i, table_url=_def_tab_url):
    '''
    Marks a job as started
    '''

    #Load table
    table = pd.read_csv(table_url, sep='\t', index_col=0)

    #Reset entry
    table.loc[i,"finished"] = False
    table.loc[i,"start_time"] = " "*len(_timef())
    table.loc[i,"finish_time"] = " "*len(_timef())
    
    #Locate index i, write to job start time
    table.loc[i,"start_time"] = _timef()

    table.to_csv(table_url, sep="\t")

@_safetyload
def finish(i, table_url=_def_tab_url):
    #MISSINGNO - print warning if job not started
    
    #Load table
    table = pd.read_csv(table_url, sep='\t', index_col=0)
    
    #Locate index i, write to job start time
    table.loc[i,"finished"] = True
    table.loc[i,"finish_time"] = _timef()

    table.to_csv(table_url, sep="\t")
    

@_safetyload
def reset(i=None, table_url=_def_tab_url):
    '''
    Resets a job
    '''
    
    table = pd.read_csv(table_url, sep='\t', index_col=0)
    table.loc[i,"finished"] = False
    table.loc[i,"start_time"] = " "*len(_timef())
    table.loc[i,"finish_time"] = " "*len(_timef())

    table.to_csv(table_url, sep="\t")

@_safetyload
def get_args(i=None, table_url=_def_tab_url):
    '''
    Returns all job parameters
    '''
    
    table = pd.read_csv(table_url, sep='\t', index_col=0)

    out = table.iloc[i].to_dict()
    out.pop("finished")
    out.pop("start_time")
    out.pop("finish_time")
    out = {"i":i} | out

    return(out)

#----------------------
def main():
    #MISSINGNO
    #Arg parsing from command line
    pass
