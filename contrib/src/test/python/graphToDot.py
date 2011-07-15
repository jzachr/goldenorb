import sys

def main():
    fdata = open(sys.argv[1])
    fdot = open(sys.argv[1] + ".dot", "w+")
    fdot.write("digraph G {\n")
    for line in fdata:
        items = line.split()
        for entry in items[1:]:
            [v,w] = entry.split(':')
            fdot.write("\t%s -> %s;\n" % (items[0], v))
    fdot.write("}\n")
    fdot.close()

if __name__ == '__main__':
    main()