from subprocess import run, call
import sys

BUFFER_SIZES = [1000, 10000, 100000, 1000000]
NP = [1, 2, 4, 8, 10]

f = open("run_all_output.txt", "w")
for bs in BUFFER_SIZES:
    for np in NP:
        call(["python3", "main_node.py", str(np), str(sys.argv[1]), str(bs), str(sys.argv[2])], stdout=f)
        f.write(f"\n\n!!!--{np}-------{bs}--!!!\n\n")
        print(f"{np} -- {bs}  DONE!")
f.close()
