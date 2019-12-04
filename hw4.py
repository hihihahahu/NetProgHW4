#!/usr/bin/env python3

from concurrent import futures
from collections import deque #for FIFO bucket
import sys  # For sys.argv, sys.exit()
import socket  # for gethostbyname()

import grpc

import csci4220_hw4_pb2
import csci4220_hw4_pb2_grpc

buckets = []

def run():
    if len(sys.argv) != 4:
        print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
        sys.exit(-1)

    global val
    global buckets
    
    local_id = int(sys.argv[1])
    my_port = str(int(sys.argv[2])) # add_insecure_port() will want a string
    k = int(sys.argv[3])
    
    #4 buckets needed
    i = 4
    while i > 0:
        #append 4 empty deques into the bucket, the deques should contain Nodes
        buckets.append(deque([]))
        i -= 1
    
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
            #print("bootstrap")
            remote_hostname = str(input_args[1])
            remote_port = int(input_args[2])
            remote_addr = socket.gethostbyname(remote_hostname)
            
            #connect to server & create stub
            channel = grpc.insecure_channel("127.0.0.1" + ':' + str(remote_port))
            stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
            
            #create the node object
            this_node = csci4220_hw4_pb2.Node(id = local_id, port = int(my_port), address = str(my_address))
            
            #call FindNode
            node_list = stub.FindNode(csci4220_hw4_pb2.IDKey(node = this_node, idkey = local_id))
            
            #add nodes from the list in node_list
            for node in node_list.nodes:
                bit_len = ((node.id)^local_id).bit_length()
                bit_len -= 1
                #pop an element if the bucket is full
                if len(buckets[bit_len]) == k:
                    buckets[bit_len].popleft()
                buckets[bit_len].append(node)
            
            #add the node that it just sent RPC to
            r_node = node_list.responding_node
            bit_len = ((r_node.id)^local_id).bit_length()
            bit_len -= 1
            if len(buckets[bit_len]) == k:
                buckets[bit_len].popleft()
            buckets[bit_len].append(r_node)
            
            #done (hopefully)
            print('After BOOTSTRAP({}), k_buckets now look like:'.format(str(r_node.id)))
            count = 0
            for bucket in buckets:
                sys.stdout.write('{}:'.format(str(count)))
                for entry in bucket:
                    sys.stdout.write(' {}:{}'.format(str(entry.id), str(entry.port)))
                sys.stdout.write('\n')
                count += 1
            
            channel.close()

        if input_args[0] == "STORE":
            print("store")
            this_key = int(input_args[1])
            this_value = input_args[2]

            closest_node = csci4220_hw4_pb2.Node(id = local_id, port = int(my_port), address = str(my_address))
            distance = abs(local_id - this_key)
            for bucket in buckets:
                for entry in bucket:
                    if abs(int(entry.id) - this_key) < distance:
                	    closest_node = entry
                	    distance = abs(int(entry.id) - this_key)
            remote_hostname = str(closest_node.id)
            remote_port = int(closest_node.port)
            remote_addr = socket.gethostbyname(remote_hostname)
            
            #connect to server & create stub
            this_addr = "127.0.0.1" + ':' + str(remote_port)
            channel = grpc.insecure_channel(this_addr)
            stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
            print(this_addr)
            node_list = stub.Store(csci4220_hw4_pb2.KeyValue(node = None, key = this_key, value = this_value))

            channel.close()
            
        if input_args[0] == "QUIT":
            break
        
        
#actual functions are implemented here, don't ask me why I created a class for it because
#the gRPC tutorial said so
class KadImplServicer(csci4220_hw4_pb2_grpc.KadImplServicer):
    def __init__(self):
        pass
    
    #Takes an ID (use shared IDKey message type) and returns k nodes with
    #distance closest to ID requested
    def FindNode(self, request, context):
    
        global buckets
    
        print('Serving FindNode({}) request for {}'.format(str(request.idkey), str(request.node.id)))
        id_in = request.idkey
        k = int(sys.argv[3])
        count = 0
        temp_list = deque([])
        #look at all Nodes in bucket
        #and insert them into the temp list
        #in the order of their distance to the
        #requested ID
        for bucket in buckets:
            for entry in bucket:
                if entry.id == request.node.id:
                    continue
                if count == 0:
                    #first entry into the temp list
                    temp_list.append(entry)
                    count += 1
                else:
                    #make sure things are sorted
                    if (int(entry.id)^int(request.idkey)) <= (int(temp_list[0].id)^int(request.idkey)):
                        deque.appendleft(entry)
                        count += 1
                    else:
                        deque.append(entry)
                        count += 1
        
        this_node = csci4220_hw4_pb2.Node(id = int(sys.argv[1]), port = int(sys.argv[2]), address = "127.0.0.1")
        node_list = None
        if count <= k:
            node_list = temp_list
        else:
            node_list = temp_list[:(k - 1)]
            
        return csci4220_hw4_pb2.NodeList(responding_node = this_node, nodes = node_list)
    
    def Store(self, request, context):
        
        global buckets
    
        print("storing something")
        val = request.value
        id_in = request.key
        
        return csci4220_hw4_pb2.IDKey(node = csci4220_hw4_pb2.Node(id = int(sys.argv[1]), port = int(sys.argv[2]), address = "127.0.0.1"), idkey = int(sys.argv[1]))
        
if __name__ == '__main__':
    run()
