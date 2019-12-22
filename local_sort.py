import sys
import time

def main():
    start_time = time.time()
    lines = open(sys.argv[1], "r", newline="\n").readlines()
    output = open(sys.argv[2], "w", newline="\n")
    output.writelines(sorted(lines, key=lambda line: line[:10]))
    #output.writelines(sorted(lines))
    output.close()
    print("[LOCAL SORT] Took ", time.time() - start_time, "seconds.")

if __name__ == "__main__":
    main()