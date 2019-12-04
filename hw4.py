#!/usr/bin/env python3

from concurrent import futures
import sys  # For sys.argv, sys.exit()
import socket  # for gethostbyname()

import grpc

import csci4220_hw4_pb2
import csci4220_hw4_pb2_grpc

def run():
    if len(sys.argv) != 4:
        print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
        sys.exit(-1)

    bucket = []
    
    local_id = int(sys.argv[1])
    my_port = str(int(sys.argv[2])) # add_insecure_port() will want a string
    k = int(sys.argv[3])
    my_hostname = socket.gethostname() # Gets my host name
    my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname
    
    #don't ask, gRPC tutorial said so
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    csci4220_hw4_pb2_grpc.add_KadImplServicer_to_server(KadImplServicer(), server)
    
    #listen from the port
    server.add_insecure_port(my_address + ':' + my_port)
    server.start()
    
	# Use the following code to convert a hostname to an IP and start a channel Note that every stub needs a channel attached to it When you are done with a channel you should call .close() on the channel. Submitty may kill your program if you have too many file descriptors open at the same time.
	
    #remote_addr = socket.gethostbyname(my_hostname)
    #remote_port = int(my_port)
    
    #channel = grpc.insecure_channel(remote_addr + ':' + str(remote_port))
    
    while True:
        input_str = str(raw_input())
        input_args = input_str.split()
        
        if input_args[0] == "BOOTSTRAP":
            print("bootstrap")
            remote_hostname = input_args[1]
            remote_port = int(input_args[2])
            remote_addr = socket.gethostbyname(remote_hostname)
            
            #connect to server
            channel = grpc.insecure_channel(remote_addr + ':' + str(remote_port))
            
            #FindNode(?)
            
        if input_args[0] == "QUIT":
            break
        
        
#actual functions are implemented here, don't ask me why I created a class for it because
#the gRPC tutorial said so
class KadImplServicer(csci4220_hw4_pb2_grpc.KadImplServicer):
    def __init__(self):
        pass
    
    def FindNode(self, request, context):
        pass
        
if __name__ == '__main__':
    run()
