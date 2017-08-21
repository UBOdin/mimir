import matplotlib.pyplot as plt
import numpy as np
import random
import sys;



#
#FUNCTIONS TO BE USED LATER:
#

#globalSetting options:
#FORMAT, KEY,XMAX,YMAX,XMIN.YMIN, PLOTNAME,XLABEL, YLABEL, and SAVEAS

#clean up the user input for Key Location to make sure that it's a valid location (default to best if not)
def cleanKeyLoc(loc):
    loc=loc.lower()
    if(loc=='upper right' or loc='ur'):
        return 'upper right'
    if(loc=='upper left' or loc='ul'):
        return 'upper left'
    if(loc=='lower left' or loc='ll'):
        return 'lower left'
    if(loc=='lower right' or loc='lr'):
        return 'lower right'
    if(loc=='right' or loc='r'):
        return 'right'
    if(loc=='center left' or loc='cl'):
        return 'center left'
    if(loc=='center right' or loc='cr'):
        return 'center right'
    if(loc=='lower center' or loc='lc'):
        return 'lower center'
    if(loc=='upper center' or loc='uc'):
        return 'upper center'
    if(loc=='center' or loc='c'):
        return 'center'
    return 'best'


#clean up the user defined input for save format to ensure that it's valid (default to png if not)
def cleanSaveFormat(saveformat):
    saveformat=saveformat.tolower()
    if(saveformat=='pdf' or saveformat=='.pdf'):
        return 'png'
    if(saveformat=='svg' or saveformat=='.svg'):
        return 'png'
    if(saveformat=='eps' or saveformat=='.eps'):
        return 'eps'
    return 'png'


#The plot function. Takes in the lineSettings and globalSettings and based on that data creates the
#desired plot
def drawPlot(lineSettings,globalSettings):
    #lineSettings is a list of lines
    #each line contains the:
    #X points [0], the Y points[1], the line-specific details [2], the X column name [3] and the Y column name [4]

    #global settings is a dictionary of globalsetting field keys with their values, default of user-defined

    #AT THIS POINT BAR GRAPH DATA IS STILL IN STRINGS AND NOT CLEANED
    plottype=globalSettings['FORMAT']
    showLegend=globalSettings['KEY']=='show'
    legendLabels=[]
    barNo=0

    #for each line, one must plot the line using its settings
    for line in lineSettings:
        xpoints=line[0]
        ypoints=line[1]
        #if there will be a legend, add a legend label for the current line
        if(showLegend):
            legendLabels.append(line[2]['LINENAME'])

        if(plottype=='line'):
            colorStyle=line[2]['COLOR']+line[2]['STYLE']
            print("line")
            plt.plot(xpoints,ypoints,colorStyle,linewidth=line[2]['WEIGHT'])
            plt.axis([(globalSettings['XMIN']),(globalSettings['XMAX']),(globalSettings['YMIN']),(globalSettings['YMAX'])])
            print("finished line")
        else:
            if(plottype=='scatter'):
                colorStyle=line[2]['COLOR']+line[2]['STYLE']
                print("scatter")
                plt.plot(xpoints,ypoints,colorStyle,markersize=line[2]['WEIGHT'])
                plt.axis([(globalSettings['XMIN']),(globalSettings['XMAX']),(globalSettings['YMIN']),(globalSettings['YMAX'])])
            else:
                if(plottype=='bar'):
                    width=0.8/len(lineSettings)
                    print("bar")
                    #if BARORIENT is horizontal, plot a horizontal bar graph
                    if globalSettings['BARORIENT']=='horizontal':
                            #xpoints are numeric, ypoints are strings
                            ypoints=list(reversed(ypoints))
                            words = np.arange(len(ypoints))
                            plt.barh(words+(barNo*width), xpoints,width, align='center', alpha=0.5,color=line[2]['COLOR'])
                            plt.yticks(words,ypoints)
                            plt.xlim(globalSettings['XMIN'],globalSettings['XMAX'])
                            barNo=barNo+1

                    #otherwise plot a vertical bar plot
                    else:
                            #xpoints are strings
                            print('--------POOT---------')
                            print(globalSettings['YMAX'])
                            print(globalSettings['YMIN'])
                            print(line[0])
                            print(line[1])
                            words = np.arange(len(xpoints))
                            plt.bar(words+(width*barNo), ypoints,width=width, align='center', alpha=0.5,color=line[2]['COLOR'])
                            plt.xticks(words,xpoints)
                            plt.ylim(globalSettings['YMIN'],globalSettings['YMAX'])
                            barNo=barNo+1
    if showLegend:
        location=cleanKeyLoc(globalSettings['KEYLOC'])
        plt.legend(legendLabels,loc=location)
    print("setting labels")
    plt.xlabel(globalSettings['XLABEL'])
    plt.ylabel(globalSettings['YLABEL'])
    plt.title(globalSettings['PLOTNAME'])
    print("saving to file")
    fileName=globalSettings['SAVENAME']+globalSettings['SAVEFORMAT']
    #once testing is done, sub in fileName for test.png
    plt.savefig('test.png',format=globalSettings['SAVEFORMAT'])
    print("saved!")
    return 0


#because for some reason, the inbuilt max and min functions don't work properly with only 2 items
#and this is faster than a long dive into why they don't do what they need to in this situation...
def getMax(val1,val2):
    val1=float(val1)
    val2=float(val2)
    if(val1>=val2):
        return val1
    else:
        print(val2)
        return val2


def getMin(val1,val2):
    val1=float(val1)
    val2=float(val2)
    if(val1<=val2):
        return val1
    else:
        return val2


# remove any data points in a non-bar graph that are not points of type (integer,integer)
def cleanAndCastData(xvals,yvals):
    cleanY=[]
    cleanX=[]
    #if either x or y value for a point triggers an exception, it does not get added to the clean lists
    #if there is no issue with either, the float values get added to the clean lists
    for i in range(0,len(yvals)):
        try:
            yCoord=float(yvals[i])
            xCoord=float(xvals[i])
            cleanY.append(yCoord)
            cleanX.append(xCoord)
        except:
            #because python gets grumpy if you try to do absolutely nothing here
            #despite the fact that that's what we need here
            x=1
    return [cleanX,cleanY]


#a cast and clean function for bar graphs, based on the value of BARORIENT in globSet
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


#fill in any missing color or style values based on what has already been used
def getUnusedColorandStyle(usedColorStyles, definedColorStyle, graphFormat):
    colorList=['k','b','r','g','y']
    #bargraphs only get colors
    if(graphFormat=='bar'):
        #find a color that hasn't been used yet
        for color in colorList:
            if color not in usedColorStyles:
                usedColorStyles.append(color)
                return [usedColorStyles,color]
        #if they've all been used, clear usedColorStyles and start repeating
        return [[colorList[0]],colorList[0]]
    #will need both color and style. Might have one already
    else:
        #styles depend on the type of graph
        if(graphFormat=='line'):
            styles=['-','--',':']
        else:
            styles= ['o','^','s']
        #if there is no color or style defined
        if(definedColorStyle==''):
            for color in colorList:
                for style in styles:
                    colorStyle=color+style
                    if colorStyle not in usedColorStyles:
                        usedColorStyles.append(colorStyle)
                        return [usedColorStyles,color,style]
            #if all the color/style combinations have been usedColorStyles
            colorStyle=colorList[0]+styles[0]
            return[[colorStyle],colorList[0],styles[0]]
        else:
            #find out which one is missing
            if definedColorStyle in styles:
                #missing color
                for color in colorList:
                    colorStyle=color+definedColorStyle
                    if colorStyle not in usedColorStyles:
                        usedColorStyles.append(colorStyle)
                        return [usedColorStyles,color,definedColorStyle]
                #if all the colors have been used for that style, pick one randomly to reapeat
                randIndex=random.randint(0,len(colorList)-1)
                usedColorStyles.append(colorList[randIndex]+definedColorStyle)
                return [usedColorStyles,colorList[randIndex],definedColorStyle]

            else:
                #missing style
                for style in styles:
                    colorStyle=definedColorStyle+style
                    if colorStyle not in usedColorStyles:
                        usedColorStyles.append(colorStyle)
                        return [usedColorStyles,definedColorStyle,style]
                #if all styles have been used for that color, pick on randomly to repeat
                randIndex=random.randint(0,len(styles)-1)
                usedColorStyles.append(definedColorStyle+styles[randIndex])
                return [usedColorStyles,definedColorStyle,styles[randIndex]]



#filter the x and y Values and return only those that correspond with filterColumn being equal to filterValue
def filterPoints(xValues,yValues,filterColumn,filterValue):
    #first remove extraneous spaces and single quotes from filterValue
    filterValue=filterValue.replace("''","").replace(" ","")
    filteredX=[]
    filteredY=[]

    for i in range(0,len(filterColumn)):
        if (filterColumn[i]==filterValue):
            filteredX.append(xValues[i])
            filteredY.append(yValues[i])
    return[filteredX,filteredY]


#Will take in the user-defined global settings
#all undefined values will be set to default values defined in the list 'default'
def addDefaultGlobalValues(definedValues,xmax,xmin,ymax,ymin):
    #DEFAULTSAVENAME is the only entry guaranteed to be in definedValues, as it will be added back in
    #the Scala code (Plot.scala). It will never be used directly, but may be used as default
    #values for other values.
    if 'SAVENAME' not in definedValues:
        saveName=definedValues['DEFAULTSAVENAME']
    else:
        saveName=definedValues['SAVENAME']
    default=[
    ('FORMAT',"line"),
    ('KEY',"show"),
    ('PLOTNAME',saveName),
    ('XLABEL',""),
    ('YLABEL',""),
    ('SAVEFORMAT',"png"),
    ('YMIN',float(ymin)*0.8),
    ('YMAX',float(ymax)*1.2),
    ('XMIN',float(xmin)*0.8),
    ('XMAX',float(xmax)*1.2),
    ('KEYLOC','best'),
    ('SAVENAME',saveName)
    ]
    #make sure that the x/y mins and maxs are all usable values.
    #If any are not, remove them and let them be replaced with defaults
    try:
        definedValues['XMAX']=float(definedValues['XMAX'])
    except:
        del definedValues['XMAX']
    try:
        definedValues['XMIN']=float(definedValues['XMIN'])
    except:
        del definedValues['XMIN']
    try:
        definedValues['YMIN']=float(definedValues['YMIN'])
    except:
        del definedValues['YMIN']
    try:
        definedValues['YMAX']=float(definedValues['YMAX'])
    except:
        del definedValues['YMAX']
    #fill in any missing values with the default values
    for value in default:
        if value[0] not in definedValues:
            definedValues[value[0]]=value[1]

    return definedValues


#convert the word style description to the corresponding symbol (if it exists, return '' if it doesn't)
def getStyleSymbol(word,plottype):
    word=word.lower()
    if(plottype=='scatter'):
        if(word=="circle" or word=="round" or word=="o"):
            return 'o'
        if(word=="square" or word=="sq" or word=="s"):
            return 's'
        if(word=="triangle" or word=="tr" or word=="^"):
            return '^'
        else:
            return ''
    else:
        if(word=="solid" or word=="line" or word=="-"):
            return '-'
        if(word=="dashed" or word=="dash" or word=="--"):
            return '--'
        if(word=="dot" or word=="dotted" or word==":"):
            return ':'
        else:
            return ''


#convert the word color description to the corresponding symbol (if it exists, return '' if it doesn't)
def getColorSymbol(word):
    word=word.lower()
    if(word=="black" or word=="blk" or word=="k"):
        return 'k'
    if(word=="blue" or word=="bl" or word=="b"):
        return 'b'
    if(word=="red" or word=="rd" or word=="r"):
        return 'r'
    if(word=="green" or word=="gn" or word=="g"):
        return 'g'
    if(word=="yellow" or word=="ylw" or word=="y"):
        return 'y'
    else:
        return ''



#
#
#
#
#   START OFF BY READING IN ALL THE DATA FROM THE SCALA PROGRAM (READ LINES FROM SYS.STDIN)
#
#
#
globSet={}
lineSet=[]
finishedData={}
split=0
#Split=0 indicates that the data being read are globalSettings
#Split=1 indicates that the data being read are line settings
#split=2 indicates that the data being read is data

openFlile=open("data.txt",'r')
for line in openFlile:
    line=line.replace("\n","")
    if(line[0]=="-"):
        split=split+1
    else:
        if(split==2):
            lineStr=""
            line=line.replace("<","").replace(">","").split(',')
            for data in line:
                data=data.split(':')
                data[0]=data[0].replace(" ","")
                if data[0] not in finishedData:
                    finishedData[data[0]]=[data[1]]
                else:
                    finishedData[data[0]].append(data[1])


        if(split==1):
            #Split on commas
            line=line.split(',')
            #strip the ( from the first division
            line[0]=line[0].replace("(","")
            settings={}
            #create a dictionary of all the settings assocaited with that line
            #(everything in 'line' after the names of the x and y columns)
            #This involves removing many extraneous characters
            for i in range (2,len(line)):
                line[i]=line[i].replace("Map(","").replace("))","").replace("->","-").split('-')
                settings[line[i][0].replace(" ","")]=line[i][1].replace("'","").replace(" ","")
            while(len(line)>3):
                line.pop(3)
            line[2]=settings
            lineSet.append(line)

        if(split==0):
            line=line.replace("(","").replace(")","").replace("'","").split(',')
            globSet[line[0]]=line[1]
openFlile.close

#
# At this point, all data is read into its proper data structures
# The next steps are
# 1. getting the actual x,y data for each line (instead of just the column names)
# 2. filling the default values wherever there is no user-defined value
#

#keep track of if the plot is a bar graph, since it may have data that is not an integer
isBar=(('FORMAT' in globSet) and (globSet['FORMAT']=='bar'))
usedStyles=[]
# initalize all the minimums and maximums to a value already in the x/y datapool
#(to avoid  situation where, say, ymax is initalized to 0, but all y data to be plotted is negative)
xmin=finishedData[lineSet[0][0]][0] #the first x value in the first line of lineSet
ymin=finishedData[lineSet[0][1]][0] #the first y value in the first line of lineSet
xmax=finishedData[lineSet[0][0]][0]
ymax=finishedData[lineSet[0][1]][0]


#replace the column names in lineSet with the actual data
#then make note of all the user-defined colors and styles (to avoid repeating colors)
#also extract the min and max for x and y of all data to be plotted--> used in globalSetting defaults

#in this loop:
#line[0] is initally = x column name (later becomes a list of values)
#line[1] is initally = y column name (later becomes a list of values)
#line[2] is a dictionary of settings for the line
#line[3] is the x column name once line[0] is overwritten with data values
#line[4] is the y column name once line[1] is overwritten with data values
for line in lineSet:
    line.append(line[0])
    line.append(line[1])
    #filter the data if the user defines any filtering
    if 'FILTER' in line[2]:
        #pointsData will contain the name of the column to filter based on, and the value from said column to filter on
        pointsData=line[2]['FILTER'].replace(" ","").split('=')
        print(pointsData)
        #gather the x and y values to filter, the column off which filtering will be based, and the filter value
        xToFilter=finishedData[line[0].replace("'","")]
        yToFilter=finishedData[line[1].replace("'","")]
        filterColumn=finishedData[pointsData[0].upper()]
        filterValue=pointsData[1]

        filteredData=filterPoints(xToFilter,yToFilter,filterColumn,filterValue)
        line[0]=filteredData[0]
        line[1]=filteredData[1]
    else:
        line[0]=finishedData[line[0].replace("'","")]
        line[1]=finishedData[line[1].replace("'","")]

    #if the plot is a scatter or line graph, all string data can be removed from the graph
    #and all points can be cast to floats
    if(not isBar):
        cleanedData=cleanAndCastData(line[0],line[1])
        line[0]=cleanedData[0]
        line[1]=cleanedData[1]
    else:
        if 'BARORIENT' not in globSet:
            globSet['BARORIENT']='vertical'
        cleanedData=castAndCleanBarData(line[0],line[1],globSet['BARORIENT'])
        line[0]=cleanedData[0]
        line[1]=cleanedData[1]
    #for each line, get the symbols for color and style(if defined).
    #also filter out invalid options
    if('COLOR' in line[2]):
        colorSymbol=getColorSymbol(line[2]['COLOR'])
        if (colorSymbol==''):
            #this might not work, may need to use a var for line[2]
            del line[2]['COLOR']
        else:
            line[2]['COLOR']=colorSymbol
        if('STYLE' in line[2]):
            if 'FORMAT' not in globSet:
                graphFormat='line'
            else:
                graphFormat=globSet['FORMAT']
            styleSymbol=getStyleSymbol(line[2]['STYLE'],graphFormat)
            if (styleSymbol==''):
                #this might not work, may need to use a var for line[2]
                del line[2]['STYLE']
            else:
                line[2]['STYLE']=styleSymbol
#if the line still has its color AND style defined, get the symbols and mark it as used
    if (('COLOR' in line[2]) and ('STYLE' in line[2])):
        usedStyles.append((line[2]['COLOR'])+(line[2]['STYLE']))

    #update the x/y min/maxs to reflect the data from this line:
    #(yvalues for the given line are line[1], x values are line[0])

    #TODO
    #find the integer values for the bar graph and use those to get the numeric column's max and min
    if(isBar):
        if globSet['BARORIENT']=='vertical':
            #x values are all strings, they don't have a numerical max or min
            xmax=0
            xmin=0
            lineYmax=max(line[1])
            lineYmin=min(line[1])
            ymax=getMax(ymax,lineYmax)
            ymin=getMin(ymin,lineYmin)
        else:
            #y values are all strings, so they have no numerical max or min
            ymin=0
            ymax=0
            lineXmax=max(line[0])
            lineXmin=min(line[0])
            xmin=getMin(xmin,lineXmin)
            xmax=getMax(xmax,lineXmax)
    else:

        lineXmax=max(line[0])
        lineYmax=max(line[1])
        lineYmin=min(line[1])
        lineXmin=min(line[0])
        xmax=getMax(xmax,lineXmax)
        ymax=getMax(ymax,lineYmax)
        xmin=getMin(xmin,lineXmin)
        ymin=getMin(ymin,lineYmin)
    print('======MAX & MIN======')
    print("xmax: "+str(xmax))
    print("ymax: "+str(ymax))
    print("xmin: "+str(xmin))
    print("ymin: "+str(ymin))
    print('--------------------')

#get all the global defaults filled in (need the graph format to determine usable style points)
globSet=addDefaultGlobalValues(globSet,xmax,xmin,ymax,ymin)


#next loop is to give default values to any settings that isn't user defined
#check if it has a color and style
#if it has both then skip
#if it's missing either then send it into a function that will find an unused combination, if possible
for line in lineSet:
    #determine what is missing from the line's settings
    missing=''
    colorStyle=''
    if('COLOR' not in line[2]):
        missing=missing+'c'
    else:
        colorStyle=colorStyle+line[2]['COLOR']
    #bar graphs should not be given a point style, as they have no points
    if(globSet['FORMAT']!='bar'):
        if('STYLE' not in line[2]):
            missing=missing+'s'
        else:
            colorStyle=colorStyle+line[2]['STYLE']
    if(missing !=''):
        colorStyleResults=getUnusedColorandStyle(usedStyles,colorStyle,globSet['FORMAT'])
        if globSet['FORMAT']=='bar':
            usedStyles=colorStyleResults[0]
            line[2]['COLOR']=colorStyleResults[1]
        else:
            usedStyles=colorStyleResults[0]
            line[2]['COLOR']=colorStyleResults[1]
            line[2]['STYLE']=colorStyleResults[2]
        #if there is not a defined value for WEIGHT, or that value is not numeric, assign the value to 1.0    
        try:
            line[2]['WEIGHT']=float(line[2]['WEIGHT'])
        except:
            line[2]['WEIGHT']=1.0
    #add a default value for lineName if it isn't defined
    if 'LINENAME' not in line[2]:
        line[2]['LINENAME']=line[3]+', '+line[4]




drawPlot(lineSet,globSet)
