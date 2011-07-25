import random
import sys

nodes = int(sys.argv[1])
maxVal = 999999
minVal = 0
edges = int(sys.argv[2])

for i in xrange(nodes):
    outputLine = str(i) + "\t" + str(random.randint(minVal,maxVal))
    for j in xrange(edges):
        outputLine = outputLine + "\t" + str(random.randint(0,nodes-1))
    if i > 0:
        outputLine = outputLine + "\t" + str(i - 1)
    print outputLine

