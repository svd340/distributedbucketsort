#!/usr/bin/env python3

import sys
import random
import socket
import ipaddress

def main():
    nodelist = []
    items = ""

    serverIP = sys.argv[1]
    serverPort = sys.argv[2]
    myPort = random.randrange(1000,9999)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((serverIP, int(serverPort)))
        s.sendall((str(myPort)).encode())
        
        data = s.recv(1024)
        #print('[NODE] Received:', repr(data), myPort)
        rawlist = data.decode()
        for node in rawlist.split("|"):
            nodelist.append((node.split(":")[0], node.split(":")[1]))

        #print("[NODE] I'm " + str(myPort) + " and my nodelist is: " + nodelist)

        while True:
            data = s.recv(1024)
            if not data:
                break 
            items += data.decode()
            #print('[NODE] Got data:', repr(data), myPort)

        print("[NODE] I'm " + str(myPort) + " and my items are: " + items, "| My nodelist is:", nodelist)


        # # Open own socket for communication with other nodes
        # commSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # try:
        #     commSock.bind((socket.gethostname(), myPort))
        # except socket.error as msg:
        #     print(str(msg))

        # commSock.listen()
        # conn, addr = s.accept() # Accept new connection on the socket
        # data = conn.recv(1024)
        # print("[NODE] Received message from node: " + data.decode())
        # #print("[NODE] Socket bound on port: " + str(myPort))


if __name__ == '__main__':
    main()