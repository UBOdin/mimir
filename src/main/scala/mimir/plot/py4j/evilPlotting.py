import matplotlib.pyplot as plt
import numpy as np

def drawPlot(xpoints,ypoints, lineColor='k-', scatterColor='ko',xaxislabel='',yaxislabel='',plottype='line',title='Fancy Graph Part II'):
    print("doin a python thing!")
    xpoints=xpoints.split(',')
    ypoints=ypoints.split(',')
    #try to convert both xpoints and ypoints to floats, if that fails, catch and convert it to strings, and set the plottype to bar
    try:
        ypoints=[float(x) for x in ypoints]
    except:
        plottype='bar'
    print("made it through the first try catch")
    try:
        xpoints=[float(x) for x in xpoints]
    except:
        plottype='bar'
    print("made it through the second try catch")
    if(plottype=='line'):
        print("line")
        plt.plot(xpoints,ypoints,lineColor)
        plt.axis([(min(xpoints)*0.8),(max(xpoints)*1.2),(min(ypoints)*0.8),(max(ypoints)*1.2)])
        print("finished line")
    else:
        if(plottype=='scatter'):
            print("scatter")
            plt.plot(xpoints,ypoints,scatterColor)
            plt.axis([(min(xpoints)*0.8),(max(xpoints)*1.2),(min(ypoints)*0.8),(max(ypoints)*1.2)])
        else:
            if(plottype=='bar'):
                print("bar")
                #if the ypoints are not numeric, plot a horizontal bar graph, assuming that xpoints are numeric
                if type(ypoints[0]) is str:
                    if ((type(xpoints[0]) is float) or (type(xpoints[0])is int)):
                        #xpoints are numbers, ypoints are words
                        words = np.arange(len(ypoints))
                        words=list(reversed(words))
                        plt.barh(words, xpoints, align='center', alpha=0.5)
                        plt.yticks(words,ypoints)
                        plt.xlim(0,(max(xpoints)*1.2))
                    else:
                        print("ERROR: no numerical data was given.")
                else:
                    if ((type(ypoints[0]) is int) or (type(ypoints[0])is float)):
                        #xpoints are words
                        words = np.arange(len(xpoints))
                        plt.bar(words, ypoints, align='center', alpha=0.5)
                        plt.xticks(words,xpoints)
                        plt.ylim(0,(max(ypoints)*1.2))

    print("setting labels")
    plt.xlabel(xaxislabel)
    plt.ylabel(yaxislabel)
    plt.title(title)
    print("saving to file")
    plt.savefig('test.png')
    print("saved!")
    #print("showing")
    #plt.show()
    #print("show finished")
