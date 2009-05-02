#!/usr/bin/env python
# Author: John Dickinson
# pastry implementation

import hashlib
import socket
import threading
import cPickle

PASTRY_BASE = 1
NEIGHBORS_TO_KEEP = 2 * (2 >> PASTRY_BASE)

PASTRY_PORT = 54321

BUFFER_SIZE = 4096

JOIN_MESSAGE = 'join'
HEARTBEAT_MESSAGE = 'heartbeat ping'

class PastryError(Exception):
    pass

class ConnectionError(PastryError):
    pass

class ProtocolError(PastryError):
    pass

# TODO: could this be optimized? surely there is a better way...
def get_common_prefix(string1, string2):
    ret = []
    for i, c in enumerate(string1):
        try:
            if string2[i] == c:
                ret.append(c)
        except IndexError:
            break
    return ''.join(ret)

class PastryNode(object):
    def __init__(self, known_host, credentials=None, application=None):
        self.credentials = credentials
        
        # the routing table is a list of lists, populated such that each row shares a common prefix
        # with this node's node_id and each column contains the node_id with the
        # (len(common prefix)+1)'th digit as one of the total possibilities
        # see the Pastry specification for more details
        self.routing_table = list()
        
        # neighborhood nodes are nodes that are known to be close in the underlying network
        self.neighborhood = list()
        
        # leaf nodes are nodes that are close in the node_id space (close in the pastry network)
        self.leafs = list()
        
        self._connection_dict = dict() # contains all connections keyed on node_id
        
        # hash the current host's ip address to get the node id
        my_ip = socket.gethostbyname(socket.gethostname())
        self.node_id = hashlib.sha1(my_ip).digest()
        
        # listen on the pastry port
        listen_location = (my_ip, PASTRY_PORT)
        self.listen_thread = threading.Thread(target=self._listen_thread, args=(listen_location,))
        self.listen_thread.start()
        
        # using the known_host, connect to the existing pastry network
        # if known_host is None, we are the first node in a new network
        if known_host is not None:
            # connect
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(known_host)
            except socket.error:
                raise ConnectionError, 'Error connecting to the server at %s' % `known_host`
            
            # send the join message with my node_id as the key
            self._send(sock, (JOIN_MESSAGE, self.node_id))
    
    def route(self, message, key):
        
        if self.leafs and min(self.leafs) < key < max(self.leafs):
            # key is in range for the leafs, send it to the closest key
            closest = self.leafs[0] # first one
            distance = abs(closest - key)
            for x in self.leafs[1:]:
                if abs(x - key) < distance:
                    distance = abs(x - key)
                    closest = x
            # forward the message to closest
            self._send(self._connection_dict[closest], (message, key))
        else:
            # key is not in range for the leafs, use the routing table
            common_prefix = get_common_prefix(key, self.node_id)
            prefix_len = len(common_prefix)
            
            try:
                next_host = self.routing_table[key[prefix_len]][prefix_len]
            except IndexError:
                next_host = None
            if next_host is None:
                # rare case
                # find the node with the node_id closest to key in the set of all nodes I know
                all_node_ids = self.leafs
                all_node_ids += self.neighborhood
                all_node_ids += [node_id for node_id in [row for row in self.routing_table] if node_id is not None]
                if all_node_ids:
                    candidates = (x for x in all_node_ids if len(get_common_prefix(x,key)) > prefix_len)
                    closest = candidates.next() # first one in the generator
                    distance = abs(closest - key)
                    for x in candidates: # the first one has already been consumed
                        if abs(x - key) < distance:
                            distance = abs(x - key)
                            closest = x
                    # forward the message to closest
                    self._send(self._connection_dict[closest], (message, key))
            else:
                # forward the message to next_host
                self._send(self._connection_dict[next_host], (message, key))
    
    def _send(self, sock, raw_data):
        print 'sending %s' % `raw_data`
        data = cPickle.dumps(raw_data)
        return sock.sendall(data)
    
    def _recv(self, sock):
        try:
            raw = sock.recv(BUFFER_SIZE)
        except socket.error:
            pass
        else:
            try:
                return cPickle.loads(raw)
            except:
                return raw
    
    def _listen_thread(self, listen_location):
        '''thread target that runs and listens for incoming connections. When new
           connections are received, spawn a thread and handle it'''
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        try:
            listen_sock.bind(listen_location)
        except socket.error, err:
            raise ConnectionError, 'Error binding to %s' % `listen_location`
        else:
            listen_sock.listen(1)
            while True:
                conn, addr = listen_sock.accept()
                handler_thread = threading.Thread(target=self._incoming_connection_handler, args=(conn, addr))
                handler_thread.setDaemon(True)
                handler_thread.start()
    
    def _incoming_connection_handler(self, conn, addr):
        while True:
            data = self._recv(conn)
            if not data:
                break
            response = self._process_received_data(data, addr)
            if response is not None:
                self._send(conn, response)
        conn.close()
    
    def _process_received_data(self, data, from_addr):
        '''handles all messages'''
        print 'received data from %s' % `from_addr`
        try:
            message, key = data
        except ValueError:
            return data
        else:
            # One eventual goal is to have each of these messages handled as a plugin.
            # The goal would be to simply add another message by writing a handler and defining
            # the message payload. In this way, the protocol can be changed or extended vary simply.
            if message == JOIN_MESSAGE:
                print 'join from %s' % key
                return 'welcome'
            elif message == HEARTBEAT_MESSAGE:
                # update last heard time for this key
                return None
            else:
                return 'unknown message'

def _testing(script_name, hostname=None, host_port=PASTRY_PORT, *extra_args):
    known_host = None
    if hostname is not None:
        known_host = (hostname, int(host_port))
    node = PastryNode(known_host)
    print node.node_id
    import time
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break
    return 0

if __name__ == '__main__':
    # testing
    import sys
    sys.exit(_testing(sys.argv[0], *sys.argv[1:]))