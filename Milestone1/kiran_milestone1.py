import yaml
from yaml.loader import SafeLoader
from datetime import datetime
import time
import pandas as pd
import copy
import threading

#Name of yaml file
filename='Milestone1B.yaml'

with open(filename) as f:
    data = yaml.load(f, Loader=SafeLoader)



def TimeFunction(FunctionInput,ExecutionTime):
    #print(1)
    time.sleep(int(ExecutionTime))
    print(int(ExecutionTime))
    return
    
def DataLoad(Filename):
    #print(2)
    DataTable=pd.read_csv(Filename)
    NoOfDefects=DataTable.shape[0]-1
    
    return DataTable, NoOfDefects

#Select correct function
def callfunction(name,inputs):
    if name=='TimeFunction':
        TimeFunction(inputs[0],inputs[1])
    elif name=='DataLoad':
        DataLoad(inputs[0])


#recursively run task while keeping track of parent task
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
            inputs.append(i)
            funs+=str(i)+', '
#         if(task['Function']=='TimeFunction'):
#             parms=str(inputs[-1])+','
#         else:
        parms=funs
                
        
        f.write(str(datetime.now())+';'+name[:-1]+' Executing ' +str(task['Function'])+' ('+parms[:-2]+')\n')
        callfunction(task['Function'],inputs)
        
    else:
        if(task['Execution']=='Sequential'):
            Activits=task['Activities']
            for i in Activits:
                par=copy.deepcopy(parents)
                par.append(i)
                runTask(Activits[i],par)
        else:
            Activits=task['Activities']         #Concurrent Execution
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