import random
import sys
i = 0

while i < 1000:
	outputLine = str(i) + "\t" + str(random.randint(0,999)) + ":" + str(random.randint(0,50))
	j = 0
	while j < 100:
		outputLine = outputLine + "\t" + str(random.randint(0,999)) + ":" + str(random.randint(0,50)) 
		j = j + 1
	print outputLine
	i = i + 1

