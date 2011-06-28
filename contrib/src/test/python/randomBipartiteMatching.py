import random
import sys
i = 0
mod = 2;

while i < 1000:
	outputLine = str(i)
	j = 0
	p = random.uniform(0.0,0.1)
	while j < 1000:
		u = random.uniform(0,1)
		if u < p and i != j:
			outputLine = outputLine + "\t" + str(j - ((i + j + 1) % 2))
		j = j + 1
	print outputLine
	i = i + 1

