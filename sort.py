import math
import random
import sys
import time
from socket_helpers import *
from helpers import *

# key:          the 10 bit ASCII key (ASCII characters in the range 32 - 126 (' ' to '~'))
# charIndex:    the index of the character in the key that is being looked at
# nodeCount:    the number of buckets the current key could be distributed to
# startNode:    the index of the first bucket in which the key could go

# Returns the index of the node that a key should be distributed to

KEY_PAYLOAD_SIZE = 100


@timethis
def hash_to_node(key, charIndex, nodeCount, startNode):
    # Calculate the size of the character range for each node (95 possible ASCII characters)
    nodeRange = 95.0 / nodeCount
    # Get the index of the node the key must go into based on the current char index
    # (minus 32 because our first key has an ASCII value of 32)
    nodeIndex = math.floor((ord(key[charIndex]) - 32.0) / nodeRange)

    # If there is more than 1 node for the current key based on the current char index
    if (nodeRange < 1):
        # Determine how many nodes there are available for the current ASCII character
        curCharNodeCount = math.ceil(nodeCount / 95.0)

        # We must move onto the next character in the key to determine which node this key goes in
        newCharIndex = charIndex + 1
        return nodeIndex + hash_to_node(key, newCharIndex, curCharNodeCount, nodeIndex)
    else:
        return nodeIndex


END = "[STOP]"


class Socket():
    def __init__(self, master_address):
        # create the connection with main node, and all other nodes
        self.master_socket = create_socket()

        self.master_socket.connect(master_address)  # set up connection with master

        self.inbound_socket = create_socket()

        self.inbound_socket.listen(10)

        self.socket_name = str(self.inbound_socket.getsockname()[0]) + ":" + str(self.inbound_socket.getsockname()[1])
        self.master_socket.sendall(self.socket_name.encode())  # send own name to master, ip:port

        self.nodes = []
        self.inbound_connections = {}
        self.outbound_sockets = {}
        self.buffer = {}
        self.input_buffer = {}
        self.data = ""
        raw_node_list = ""

        while True:
            # receive the data from the master node
            packet = self.master_socket.recv(1).decode()
            if packet == "!":
                break
            raw_node_list += packet

        for node in raw_node_list.split("|"):
            self.nodes.append((node.split(":")[0], int(node.split(":")[1])))

        for addr, port in self.nodes:
            # create connections to all other nodes
            outbound_socket = create_socket()
            self.outbound_sockets[(addr, port)] = outbound_socket
            outbound_socket.connect((addr, port))

        for i in range(len(self.nodes)):
            # accept connections to all other nodes
            while True:
                try:
                    conn, addr = self.inbound_socket.accept()  # Accept new connection on the socket
                    self.inbound_connections[addr] = conn
                    self.input_buffer[conn] = ""

                    conn.setblocking(False)
                    break
                except:
                    print("[ACCEPT ERROR] ")

    @timethis
    def get_from_main(self):
        # get all the data from the main node
        total_text_data = ""
        while True:
            data = self.master_socket.recv(BUFFSIZE).decode()
            total_text_data += data
            if total_text_data[-len(END):] == END:
                total_text_data = total_text_data[:-len(END)]
                break

        string_tuples = total_text_data.splitlines()
        tuples = []
        for string_tuple in string_tuples:
            # print("String tuple", string_tuple)
            tuples.append((string_tuple[:10], string_tuple[10:].strip()))
        return tuples

    def post_to_node_index(self, node_index, data):
        # sent to the node belonging to the corresponding index
        self.post(self.nodes[node_index][0], self.nodes[node_index][1], data)

    @timethis
    def post(self, ip, port, data):
        if not (ip, port) in self.buffer:
            self.buffer[(ip, port)] = []
        self.buffer[(ip, port)].append(data)

        # we want to send large packages, to not overload the network.
        # to achieve this we first store all data in a buffer
        if len(self.buffer[(ip, port)]) > BUFFSIZE / KEY_PAYLOAD_SIZE:
            self.outbound_sockets[(ip, port)].sendall("".join(self.buffer[(ip, port)]).encode())
            self.buffer[(ip, port)] = []
            self.load_buffer()

    def empty_buffers(self):
        # send all data that was remaining in the buffers
        for (ip, port), data in self.buffer.items():
            self.outbound_sockets[(ip, port)].sendall("".join(data).encode())
            self.buffer[(ip, port)] = []

    @timethis
    def post_to_master(self, data):
        # we divide the data in packages that can be properly sent over the network

        for x in range(math.ceil(len(data) / BUFFSIZE)):
            self.master_socket.sendall(data[x * BUFFSIZE:x * BUFFSIZE + BUFFSIZE].encode())
        self.master_socket.sendall(END.encode())

    @timethis
    def tell_done(self):
        # tell all other nodes that no more data is coming from the current node
        self.empty_buffers()
        for connection in self.outbound_sockets.values():
            connection.sendall(END.encode())
            connection.shutdown(socket.SHUT_RDWR)
            connection.close()

    @timethis
    def load_buffer(self, buffer_size=BUFFSIZE):
        # this method loads the socket into buffers that can be returned with the .get() call.
        # this to ensure we do not overload the socket buffers, and can fit all data in memory normally

        for key, connection in list(self.inbound_connections.items()):
            try:
                packet = connection.recv(buffer_size).decode()
                self.input_buffer[connection] += packet

                # check whether the END package has been received.
                if self.input_buffer[connection][-len(END):] == END:
                    self.input_buffer[connection] = self.input_buffer[connection][:-len(END)]
                    del self.inbound_connections[key]
                    connection.shutdown(socket.SHUT_RDWR)
                    connection.close()
                    continue

            except BlockingIOError as e:
                # because this is a non blocking socket, it is mostly empty, and will return a blockingIO Error
                pass

    def get(self):
        # load all data, end return the dataset as a long string
        while len(self.inbound_connections) > 0:
            self.load_buffer()

        self.data = "".join(self.input_buffer.values())
        return self.data

    def node_count(self):
        return len(self.nodes)

    def get_nodes(self):
        return self.nodes


class Sort:
    def __init__(self, node_index, master_address):
        self.data = None
        self.socket = Socket(master_address)
        self.node_count = self.socket.node_count()
        self.index = node_index

    def read_data(self):
        # the sent/received data should be tuples of keys
        self.data = self.socket.get_from_main()

    def redistribute(self):
        # redistribute the data according to the bucket sort algorithm to the correct nodes
        hashtime = 0
        posttonodetime = 0
        for key, data in self.data:
            t = time.time()

            # index of the node to send to according to the key
            process_index = hash_to_node(key, 0, self.node_count, 0)
            hashtime += time.time() - t

            # post the data to the correct node
            t = time.time()
            self.socket.post_to_node_index(process_index, str(key) + " " + str(data) + "\n")
            posttonodetime += time.time() - t

        print("[HASHTIME] %s" % hashtime)
        print("[POSTTONODETIME] %s" % posttonodetime)

        self.socket.tell_done()

    def gather(self):
        # gather all data that has been sent to this node
        self.data = self.socket.get()
        temp = []

        self.data = self.data.split("\n")
        for datapoint in self.data:
            if not datapoint.strip(): continue
            temp.append((datapoint[:10], datapoint[10:].strip()))

        self.data = temp

    @timethis
    def sort(self):
        # sort the data
        sorttime = time.time()
        self.data = list(sorted(self.data, key=lambda kv: kv[0]))
        print("[SORT TIME] %s" % (time.time() - sorttime))

    @timethis
    def return_data(self):
        # return the data to the main node
        s = ""

        for t in self.data:
            s += str(t[0]) + "  " + str(t[1]) + " \n"

        self.socket.post_to_master(s)

    def execute(self):
        # the main flow of the program, receive data, redistribute, gather, sort, send back to main

        t = time.time()
        self.read_data()
        print("[READ DATA TIME] %s" % (time.time() - t))

        t = time.time()
        self.redistribute()
        print("[REDISTRIBUTE TIME] %s" % (time.time() - t))

        t = time.time()
        self.gather()
        print("[GATHER TIME] %s" % (time.time() - t))

        self.sort()
        self.return_data()


if __name__ == '__main__':
    sort = Sort(0, (sys.argv[1], int(sys.argv[2])))
    sort.execute()
    print_timethis()
