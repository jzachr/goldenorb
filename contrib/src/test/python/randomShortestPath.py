import random

def main():
    for i in range(1000):
        out = ''
        for j in range(10):
            v = random.randint(0,999)
            while v is j:
                v = random.randint(0,999)
            w = random.randint(10,20)
            out += "\t%d:%d" % (v,w)
        print "%d%s" % (i,out)

if __name__ == '__main__':
    main()

