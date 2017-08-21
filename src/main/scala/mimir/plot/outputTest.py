import sys;
import matplotlib.pyplot as plt
import random

#a cast and clean function for bar graphs, based on the value of BARORIENT in

def castAndCleanBarData(xvals,yvals,orientation):
    if(orientation=='vertical'):
        barNames=xvals
        barVals=yvals
    else:
        barNames=yvals
        barVals=xvals
    cleanNames=[]
    cleanVals=[]
    #try to cast the each value in the numeric column to a float. If there are no exceptions raised,
    #then append that value and its corresponding string column value to the cleaned value lists
    #If an exception is raised, then do nothing with that point and move on to the next one
    for i in range(0,len(barVals)):
        try:
            num=float(barVals[i])
            cleanVals.append(num)
            cleanNames.append(barNames[i])
        except:
            #do nothing, but python won't let you actually do nothing, so:
            placeholder=1
    if(orientation=='vertical'):
        return [cleanNames,cleanVals]
    else:
        return [cleanVals,cleanNames]





x=['1','2','3','4','5']
y=['1','2','3','4','5']
vals=castAndCleanBarData(x,y,'horizontal')
print(vals[0])
print(vals[1])
