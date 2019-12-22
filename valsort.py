from subprocess import run,call
import sys

count = int(sys.argv[1])
for x in range(count):
    call(["./valsort", "-o", "sum{}.txt".format(x),"output{}.txt".format(x)])

f = open("all.sum","w")

call(["cat", *["sum{}.txt".format(i) for i in range(count)],],stdout=f)

run(["./valsort", "-s", "all.sum"])
