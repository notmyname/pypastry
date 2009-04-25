# pastry application

class PastryApplication(object):
    def deliver(self, message, key):
        raise NotImplementedError
    
    def forward(self, message, key, next_id):
        raise NotImplementedError
    
    def newLeafs(self, leaf_set):
        raise NotImplementedError