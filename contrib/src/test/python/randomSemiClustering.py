import random
import sys
edge_i,edge_j = 1000,1000
edges = [[0]*edge_j for i in range(edge_i) ]
i = 0

while i < 1000:
	outputLine = str(i)
	j = 0
	p = random.uniform(0.0,0.05)
	while j < 1000:
		u = random.uniform(0,1)
		if u < p and i != j:
			weight = random.uniform(0,1)		
			edges[i][j] = weight
			edges[j][i] = weight
		j = j + 1
	i = i + 1

i = 0
while i < 1000:
	outputLine = str(i)
	j = 0
	while j < 1000:
		u = random.uniform(0,1)
		if edges[i][j] > 0:
			outputLine = outputLine + "\t" + str(j) + ":" + str(edges[i][j])
		j = j + 1
	print outputLine
	i = i + 1

