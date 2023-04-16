import networkx as nx
from itertools import product


"""
>>> dag = DAG()
>>> A = dag.param("A", ["a1", "a2", "a3"])
>>> B = dag.param("B", ["b1", "b2"])
>>>  C = dag.param("C", ["c1", "c2"])
>>> A >> B
>>> B >> C
>>> print(dag)
Task(A=a1, B=b1, C=c1)
Task(A=a1, B=b1, C=c2)
Task(A=a1, B=b2, C=c1)
Task(A=a1, B=b2, C=c2)
Task(A=a2, B=b1, C=c1)
Task(A=a2, B=b1, C=c2)
Task(A=a2, B=b2, C=c1)
Task(A=a2, B=b2, C=c2)
Task(A=a3, B=b1, C=c1)
Task(A=a3, B=b1, C=c2)
Task(A=a3, B=b2, C=c1)
Task(A=a3, B=b2, C=c2)

>>> dag = DAG()
>>> A = dag.param("A", ["a1", "a2"])
>>> B = dag.param("B", ["b1", "b2"])
>>> C = dag.param("C", ["c1", "c2"])
>>> A >> C
>>> B >> C
>>> print(dag)
Task(A=a1, C=c1)
Task(A=a1, C=c2)
Task(A=a2, C=c1)
Task(A=a2, C=c2)
Task(B=b1, C=c1)
Task(B=b1, C=c2)
Task(B=b2, C=c1)
Task(B=b2, C=c2)

>>> print("Fork DAG:")
>>> dag = DAG()
>>> A = dag.param("A", ["a1", "a2"])
>>> B = dag.param("B", ["b1", "b2"])
>>> C = dag.param("C", ["c1", "c2"])
>>> A >> B
>>> A >> C
>>> print(dag)
Fork DAG:
Task(A=a1, B=b1)
Task(A=a1, B=b2)
Task(A=a2, B=b1)
Task(A=a2, B=b2)
Task(A=a1, C=c1)
Task(A=a1, C=c2)
Task(A=a2, C=c1)
Task(A=a2, C=c2)

>>> dag = DAG()
>>> A = dag.param("A", ["a1", "a2"])
>>> B = dag.param("B", ["b1", "b2"])
>>> C = dag.param("C", ["c1", "c2"])
>>> D = dag.param("D", ["d1", "d2"])
>>> A >> B
>>> A >> C
>>> B >> D
>>> C >> D
print(dag)
Diamond:
Task(A=a1, B=b1, D=d1)
Task(A=a1, B=b1, D=d2)
Task(A=a1, B=b2, D=d1)
Task(A=a1, B=b2, D=d2)
Task(A=a1, C=c1, D=d1)
Task(A=a1, C=c1, D=d2)
Task(A=a1, C=c2, D=d1)
Task(A=a1, C=c2, D=d2)
Task(A=a2, B=b1, D=d1)
Task(A=a2, B=b1, D=d2)
Task(A=a2, B=b2, D=d1)
Task(A=a2, B=b2, D=d2)
Task(A=a2, C=c1, D=d1)
Task(A=a2, C=c1, D=d2)
Task(A=a2, C=c2, D=d1)
Task(A=a2, C=c2, D=d2)

>>> dag = DAG()
>>> A = dag.param("A", ["a1", "a2"])
>>> B = dag.param("B", ["b1", "b2"])
>>> C = dag.param("C", ["c1", "c2"])
>>> A >> B
>>> B >> C
>>> print("Series / Chain:")
>>> print(dag)
Task(A=a1, B=b1, C=c1)
Task(A=a1, B=b1, C=c2)
Task(A=a1, B=b2, C=c1)
Task(A=a1, B=b2, C=c2)
Task(A=a2, B=b1, C=c1)
Task(A=a2, B=b1, C=c2)
Task(A=a2, B=b2, C=c1)
Task(A=a2, B=b2, C=c2)


>>> dag = DAG()
>>> A = dag.param("A", ["a1", "a2"])
>>> B = dag.param("B", ["b1", "b2"])
>>> C = dag.param("C", ["c1", "c2"])
>>> D = dag.param("D", ["d1", "d2"])
>>> A >> B
>>> B >> C
>>> C >> D
>>> A >> D
>>> print(dag)
Task(A=a1, B=b1, C=c1, D=d1)
Task(A=a1, B=b1, C=c1, D=d2)
Task(A=a1, B=b1, C=c2, D=d1)
Task(A=a1, B=b1, C=c2, D=d2)
Task(A=a1, B=b2, C=c1, D=d1)
Task(A=a1, B=b2, C=c1, D=d2)
Task(A=a1, B=b2, C=c2, D=d1)
Task(A=a1, B=b2, C=c2, D=d2)
Task(A=a2, B=b1, C=c1, D=d1)
Task(A=a2, B=b1, C=c1, D=d2)
Task(A=a2, B=b1, C=c2, D=d1)
Task(A=a2, B=b1, C=c2, D=d2)
Task(A=a2, B=b2, C=c1, D=d1)
Task(A=a2, B=b2, C=c1, D=d2)
Task(A=a2, B=b2, C=c2, D=d1)
Task(A=a2, B=b2, C=c2, D=d2)
Task(A=a1, D=d1)
Task(A=a1, D=d2)
Task(A=a2, D=d1)
Task(A=a2, D=d2)


>>> from dag import DAG
>>> dag = DAG()
>>> A = dag.param("A", ["a1"])
>>> B = dag.param("B", ["b1"])
>>> C = dag.param("C", ["c1"])
>>> D = dag.param("D", ["d1"])
>>> A >> B
>>> A >> C
>>> B >> D
>>> C >> D
>>> A >> D
>>> print(dag)
Task(A=a1, B=b1, D=d1)
Task(A=a1, C=c1, D=d1)
Task(A=a1, D=d1)
"""

class TaskIterator:
    def __init__(self, tasks):
        self.tasks = tasks
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < len(self.tasks):
            task = self.tasks[self.index]
            self.index += 1
            return task
        else:
            raise StopIteration

class Node:
    def __init__(self, name, dag):
        self.name = name
        self.dag = dag

    def __rshift__(self, other):
        self.dag.link(self, other)
        return other

    def __repr__(self):
        return self.name
    
class Task:
    def __init__(self, task_params):
        self.task_params = task_params

    def __repr__(self):
        return "Task(" + ", ".join([f"{k}={v}" for k, v in self.task_params.items()]) + ")"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if not isinstance(other, Task):
            return False
        return self.task_params == other.task_params

    def __hash__(self):
        return hash(tuple(sorted(self.task_params.items())))
    
class GraphOperations(nx.DiGraph):
    def get_all_paths(self, start_node, end_node):
        return list(nx.all_simple_paths(self, start_node, end_node))
    
import uuid

class DAG(GraphOperations):
    
    def __init__(self):
        super().__init__()
        self.params = {}
        self.layers = []
        
    def add_param(self, name, values=None, link_to=None):
        if isinstance(values,str):
            values = values.split(",")
        if values is None:
            values = [name]
        self.add_node(name)
        self.params[name] = values
        self.layers.append(name)
        new_node = Node(name, self)
        if link_to:
            self.add_edge(link_to.name, new_node.name)
        return new_node
    
    def param(self, name, values=None):
        return self.add_param(name, values, False)

    def link(self, from_node, to_node):
        self.add_edge(from_node.name, to_node.name)
        

    def cartesian_product(self, nodes):
        return list(product(*[self.params[node] for node in nodes]))
    
    def get_all_paths(self, start_nodes, end_nodes):
        start_node_names = [node.name for node in start_nodes]
        end_node_names = [node.name for node in end_nodes]
        all_paths = []
        for start_node in start_node_names:
            for end_node in end_node_names:
                all_paths.extend(super().get_all_paths(start_node, end_node))
        return all_paths
    
    def get_start_nodes(self):
        start_node_names = [node for node, in_degree in self.in_degree if in_degree == 0]
        return [Node(name, self) for name in start_node_names]
    

    def get_end_nodes(self):
        end_node_names = [node for node, out_degree in self.out_degree if out_degree == 0]
        return [Node(name, self) for name in end_node_names]
    
    def __str__(self):
        start_nodes = self.get_start_nodes()
        end_nodes = self.get_end_nodes()
        all_paths = self.get_all_paths(start_nodes, end_nodes)
        return '\n'.join(map(str, self.generate_tasks(all_paths)))
    
    def generate_tasks(self, all_paths):
        tasks = [
            Task({path[i]: combo[i] for i in range(len(combo))})
            for path in all_paths
            for combo in self.cartesian_product(path)
        ]
        return tasks

    def task_iterator(self):
        all_paths = self.get_all_paths(self.get_start_nodes(), self.get_end_nodes())
        return TaskIterator(self.generate_tasks(all_paths))
    
    @property
    def tasks(self):
        return list(self.task_iterator())
