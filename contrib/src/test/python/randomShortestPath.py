import random

nodes = 100000
edges_per_node = 100

def main():
    for i in range(nodes):
        out = ''
        for j in range(edges_per_node):
            v = random.randint(0,nodes-1)
            while v is j:
                v = random.randint(0,nodes-1)
            w = random.randint(10,20)
            out += "\t%d:%d" % (v,w)
        print "%d%s" % (i,out)

if __name__ == '__main__':
    main()

