#!/usr/bin/env python3

import sys
import os
import socket
import math
import time
import sort

from socket_helpers import *
from subprocess import Popen

# SERVER_PORT = 7001
PROCESS_COUNT = int(sys.argv[1])  # process count is from the first command line arg
END = "[STOP]"


def main():
    start_time = time.time()
    print("[SERVER] My IP is: " + get_ip())

    s = create_socket()


    try:
        print("[SERVER] Socket bound on port: " + str(s.getsockname()))
        print("[SERVER] Opening " + str(PROCESS_COUNT) + " processes...")
        pruntime = time.time()
        # for i in range(PROCESS_COUNT):
        #     Popen(["python3", "sort.py",str(i), s.getsockname()[0], str(s.getsockname()[1])])
        Popen(["prun", "-np", sys.argv[1], "python3", "sort.py", str(0), s.getsockname()[0], str(s.getsockname()[1])])

        connections = []
        print("[PRUN TIME] %s" % (time.time() - pruntime))

        s.listen(10)

        for i in range(PROCESS_COUNT):
            conn, addr = s.accept()  # Accept new connection on the socket
            # print('[SERVER] Received connection from: ', addr)
            data = conn.recv(1024)
            # conn.setblocking(False)
            # print("[SERVER] Received message from node: " + data.decode())
            connections.append(
                ((conn, addr), data.decode().split(":")))  # main node to send to , child node receiving address

        # print("[CONNECTIONS] ", connections)

        nodelist = ""
        for node in connections:
            nodelist += str(node[1][0]) + ":" + node[1][1] + "|"
        nodelist = nodelist[:-1] + "!"
        print("[NODELIST] " + nodelist)
        for i, connection in enumerate(connections, start=0):
            # for connection in connections:
            conn = connection[0][0]
            conn.sendall((nodelist).encode())

        sendtime = time.time()
        print("[SERVER] Sending data to nodes")
        with open(sys.argv[2], 'r') as f:
            cnt = 0
            line = f.readline()
            while line:
                connections[cnt % PROCESS_COUNT][0][0].sendall(line.encode())
                cnt += 1
                line = f.readline()
        print("[SEND TIME] %s" % (time.time() - sendtime))

        for idx in range(PROCESS_COUNT):
            connections[idx][0][0].sendall(END.encode())
            connections[idx][0][0].setblocking(True)

            # lines_per_node = math.ceil(len(lines) / len(connections))
            # for line_index, line in enumerate(lines[i * (lines_per_node):], start=i * lines_per_node):
            #     if line_index >= (i * lines_per_node) + lines_per_node - 1 or line_index >= len(lines) - 1:
            #         conn.sendall((line + "[STOP]").encode())
            #         break
            #     else:
            #         conn.sendall(line.encode())

        print("[SERVER] Gathering data from nodes... ")
        waittime = time.time()
        gathertime = None
        final = ""
        for i, connection in enumerate(connections, start=0):
            total_data = []
            data = ""
            total_text_data = ""
            while True:
                data = connection[0][0].recv(8192).decode()
                if gathertime is None:
                    gathertime = time.time()

                if END in data:
                    total_data.append(data[:data.find(END)])
                    break
                total_data.append(data)
                if len(total_data) > 1:
                    last_pair = total_data[-2] + total_data[-1]
                    if END in last_pair:
                        total_data[-2] = last_pair[:last_pair.find(END)]
                        total_data.pop()
                        break
            total_text_data = ''.join(total_data)

            # packet = connection[0][0].recv(1024).decode()[:-6]
            final += total_text_data
        print("[WAIT TIME] %s" % (time.time() - waittime))
        print("[GATHER TIME] %s" % (time.time() - gathertime))

        with open("output.txt", 'w') as f:
            f.write(final)

        print("[SERVER] Final output written ")
        print("--- %s seconds ---" % (time.time() - start_time))

    # conn.sendall(("aaaa|payloooooad\nbbbb|lolololo\ncccc|testing[STOP]").encode())
    except Exception as e:
        import traceback
        traceback.print_exc()
    finally:
        print("always run this!!!")
        s.shutdown(socket.SHUT_RDWR)
        s.close()


import fcntl
import struct





if __name__ == '__main__':
    main()
