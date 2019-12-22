import socket
import fcntl
import struct
import sys



try:
    BUFFSIZE = int(sys.argv[3])
except:
    BUFFSIZE = 1000000

IFNAME = 'ib0'
# IFNAME = 'lo'
def create_socket(ifname=IFNAME):
    # this function creates a socket, and binds it to a specified device
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.bind((get_ip(ifname),0))

    return s


def get_ip(ifname=IFNAME):
    #this gets the ip of the specified NIC
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', bytes(ifname[:15], 'utf-8'))
    )[20:24])