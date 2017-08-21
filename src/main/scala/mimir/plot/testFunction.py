import sys

outFile=open("src/main/scala/mimir/plot/data.txt",'w')
for dataPoint in sys.stdin:
    outFile.write(dataPoint)
outFile.close()
print('done')
