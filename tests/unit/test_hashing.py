import pytest
from src.utils.hashing import ConsistentHashing

def test_consistent_hashing_distribution():
    nodes = ["http://node-1:8000", "http://node-2:8000", "http://node-3:8000"]
    ch = ConsistentHashing(nodes=nodes, replicas=10)
    
    # Check that keys are distributed
    topic1_node = ch.get_node("topic1")
    topic2_node = ch.get_node("topic2")
    
    assert topic1_node in nodes
    assert topic2_node in nodes
    
def test_node_removal():
    nodes = ["node1", "node2"]
    ch = ConsistentHashing(nodes=nodes)
    
    first_node = ch.get_node("some_key")
    ch.remove_node("node1" if first_node == "node1" else "node2")
    
    second_node = ch.get_node("some_key")
    assert second_node != first_node
    assert second_node in nodes
