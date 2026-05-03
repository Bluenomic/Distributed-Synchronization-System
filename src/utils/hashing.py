import hashlib
import bisect

class ConsistentHashing:
    def __init__(self, nodes=None, replicas=3):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        
        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        """Adds a node to the hash ring with multiple replicas."""
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            bisect.insort(self.sorted_keys, key)

    def remove_node(self, node):
        """Removes a node and its replicas from the hash ring."""
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            del self.ring[key]
            self.sorted_keys.remove(key)

    def get_node(self, string_key):
        """Given a string key, returns the node it maps to."""
        if not self.ring:
            return None
        
        key = self._hash(string_key)
        idx = bisect.bisect(self.sorted_keys, key)
        
        # If idx is at the end, wrap around to the first node
        if idx == len(self.sorted_keys):
            idx = 0
            
        return self.ring[self.sorted_keys[idx]]

    def _hash(self, key):
        """Returns an integer hash for a string."""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
