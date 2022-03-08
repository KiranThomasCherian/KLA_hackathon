import yaml
from yaml.loader import SafeLoader
from datetime import datetime
import time
import pandas as pd
import copy
import threading


#Filename
filename='Milestone2B.yaml'

with open(filename) as f:
    data = yaml.load(f, Loader=SafeLoader)


#keep track of return values for each task
returnValues={}

def TimeFunction(FunctionInput,ExecutionTime):
    #print(1)
    
    time.sleep(int(ExecutionTime))
    print(int(ExecutionTime))
    return
    
def DataLoad(Filename):
    #print(2)
    DataTable=pd.read_csv(Filename)
    NoOfDefects=DataTable.shape[0]
    
    return DataTable, NoOfDefects

#call correct function
def callfunction(name,inputs,pname):
    if name=='TimeFunction':
        TimeFunction(inputs[0],inputs[1])
    elif name=='DataLoad':
        ret1,ret2=DataLoad(inputs[0])
        returnValues[pname+'DataTable']=ret1
        returnValues[pname+'NoOfDefects']=ret2

##recursively run task while keeping track of parent task
def runTask(task,parents):
    #print(task)
    name=''
    for i in parents:
        name+=str(i)+'.'
    
    f.write(str(datetime.now())+';'+name[:-1]+' Entry\n')
    if task['Type']=='Task':
        funs=''
        parms=''
        inputs=[]

        for i in task['Inputs'].values():
            if i[0]=='$':
                while(i[2:-1] not in returnValues.keys()):          #wait till value becomes available
                    time.sleep(0.25)
                    continue
                inputs.append(returnValues[i[2:-1]])
                funs+=str(returnValues[i[2:-1]])+','
            else:
                inputs.append(i)
                funs+=str(i)+', '
#         if(task['Function']=='TimeFunction'):
#             parms=str(inputs[-1])+','
#         else:
        parms=funs

        if 'Condition' in task.keys():
            #print(task['Condition'])
            arg1=task['Condition'].split()[0][2:-1]
            arg2=int(task['Condition'].split()[-1])
            op=task['Condition'].split()[1]

            if(op=='>'):
                if returnValues[arg1]>arg2:
                    f.write(str(datetime.now())+';'+name[:-1]+' Executing ' +str(task['Function'])+' ('+parms[:-2]+')\n')
                    callfunction(task['Function'],inputs,name)
                else:
                    f.write(str(datetime.now())+';'+name[:-1]+' Skipped\n')

            elif(op=='<'):
                if returnValues[arg1]<arg2:
                    f.write(str(datetime.now())+';'+name[:-1]+' Executing ' +str(task['Function'])+' ('+parms[:-2]+')\n')
                    callfunction(task['Function'],inputs,name)
                else:
                    f.write(str(datetime.now())+';'+name[:-1]+' Skipped\n')
        else:    
            f.write(str(datetime.now())+';'+name[:-1]+' Executing ' +str(task['Function'])+' ('+parms[:-2]+')\n')
            callfunction(task['Function'],inputs,name)
        
    else:
        if(task['Execution']=='Sequential'):
            Activits=task['Activities']
            for i in Activits:
                par=copy.deepcopy(parents)
                par.append(i)
                runTask(Activits[i],par)
        else:
            Activits=task['Activities']                 #concurrent
            threads=[]
            for i in Activits:
                par=copy.deepcopy(parents)
                par.append(i)
                t=threading.Thread(target=runTask, args=(Activits[i],par,))
                threads.append(t)
                t.start()
            for t in threads:
                t.join()
    f.write(str(datetime.now())+';'+name[:-1]+' Exit\n')
        
#____________________
#Generating logs

baseTask=list(data.keys())[0]

f = open(filename[:-5]+"_Log.txt", "w")
f.write(str(datetime.now())+';'+baseTask+' Entry\n')

baseActivities=data[baseTask]['Activities']
#print(baseActivities)
for i in baseActivities:
    runTask(baseActivities[i],[baseTask,i])

f.write(str(datetime.now())+';'+baseTask+' Exit')
f.close()

print("Generated Log file in same directory with same name+_log.txt")