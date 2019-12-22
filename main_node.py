#!/usr/bin/env python3

import sys
import os
import math
import time
import sort

from socket_helpers import *
from subprocess import Popen

# SERVER_PORT = 7001
PROCESS_COUNT = int(sys.argv[1])  # process count is from the first command line arg
END = "[STOP]"
USERNAME = sys.argv[4]
RAMSIZE = 10000000000


def main():
    start_time = time.time()
    print("[SERVER] My IP is: " + get_ip())

    s = create_socket()

    try:
        print("[SERVER] Socket bound on port: " + str(s.getsockname()))
        print("[SERVER] Opening " + str(PROCESS_COUNT) + " processes...")
        pruntime = time.time()

        print("[MASTER]", s)
        Popen(["prun", "-np", sys.argv[1], "python3", "sort.py", s.getsockname()[0], str(s.getsockname()[1]),
               str(USERNAME)])

        connections = []
        print("[PRUN TIME] %s" % (time.time() - pruntime))

        s.listen(10)

        for i in range(PROCESS_COUNT):
            # setup a connection with all nodes
            conn, addr = s.accept()  # Accept new connection on the socket
            data = conn.recv(1024)
            connections.append(
                ((conn, addr), data.decode().split(":")))  # main node to send to , child node receiving address
        all_nodes_ready = time.time()

        nodelist = ""
        for node in connections:
            nodelist += str(node[1][0]) + ":" + node[1][1] + "|"
        nodelist = nodelist[:-1] + "!"
        print("[NODELIST] " + nodelist)
        for i, connection in enumerate(connections, start=0):
            # for connection in connections:
            conn = connection[0][0]
            conn.sendall((nodelist).encode())

        print("[SERVER] Sending data to nodes")
        totalsendtime = time.time()
        actual_send_time = 0
        with open(sys.argv[2], 'r', newline="\n") as f:

            x = 0
            # line = f.readline()
            pcnt = 0
            while True:
                # distributed the data over all nodes
                data = f.read(RAMSIZE)  # this should be the ram size
                if not data: break
                while x * BUFFSIZE < len(data):
                    sendtime = time.time()
                    connections[pcnt % PROCESS_COUNT][0][0].sendall(data[x * BUFFSIZE:(x + 1) * BUFFSIZE].encode())
                    actual_send_time += time.time() - sendtime
                    x += 1
                    pcnt += 1

        print("[SENDING TIME] %s" % (time.time() - totalsendtime))
        print("[ACTUAL SEND TIME] %s" % (actual_send_time))

        for idx in range(PROCESS_COUNT):
            connections[idx][0][0].sendall(END.encode())
            connections[idx][0][0].setblocking(False)

        print("[SERVER] Gathering data from nodes... ")
        waittime = time.time()
        gathertime = None
        skip_index = set()

        # emptying all files
        for i in range(len(connections)):
            with open(f"/var/scratch/{USERNAME}/output{i}.txt", 'w') as f:
                pass

        while len(skip_index) < PROCESS_COUNT:
            for i in range(len(connections)):
                if i in skip_index: continue
                try:
                    # receive the data that is ready to be received
                    connection = connections[i]
                    data = connection[0][0].recv(BUFFSIZE).decode()
                    if gathertime is None:
                        gathertime = time.time()
                    if END in data:
                        data = data[:-len(END)]
                        skip_index.add(i)
                    #write to the dsys scratch dir
                    with open(f"/var/scratch/{USERNAME}/output{i}.txt", 'a') as f:
                        f.write(data)
                        first = False
                except BlockingIOError:
                    # we do this in an async manner, so the first socket that is ready can start sending
                    pass
                except Exception as e:
                    print(e)

        print("[WAIT TIME] %s" % (time.time() - waittime))
        print("[GATHER TIME] %s" % (time.time() - gathertime))

        print("[SERVER] Final output written ")
        print("--- %s seconds ---" % (time.time() - start_time))
        print("After all nodes ready--- %s seconds ---" % (time.time() - all_nodes_ready))

    except Exception as e:
        import traceback
        traceback.print_exc()
    finally:
        print("always run this!!!")
        s.close()


if __name__ == '__main__':
    main()
