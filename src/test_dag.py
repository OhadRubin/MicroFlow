



import pytest

# class TestDAG(unittest.TestCase):

def test_simple_dag():
    from dag import DAG


    dag = DAG()
    A = dag.param("A", ["a1", "a2", "a3"])
    B = dag.param("B", ["b1", "b2"])
    C = dag.param("C", ["c1", "c2"])
    A >> B
    B >> C
    str(dag)=="""Task(A=a1, B=b1, C=c1)
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
Task(A=a3, B=b2, C=c2)"""


    dag = DAG()
    A = dag.param("A", ["a1", "a2"])
    B = dag.param("B", ["b1", "b2"])
    C = dag.param("C", ["c1", "c2"])
    A >> C
    B >> C
    assert str(dag)=='Task(A=a1, C=c1)\nTask(A=a1, C=c2)\nTask(A=a2, C=c1)\nTask(A=a2, C=c2)\nTask(B=b1, C=c1)\nTask(B=b1, C=c2)\nTask(B=b2, C=c1)\nTask(B=b2, C=c2)'




    dag = DAG()
    A = dag.param("A", ["a1", "a2"])
    B = dag.param("B", ["b1", "b2"])
    C = dag.param("C", ["c1", "c2"])
    A >> B
    A >> C
    assert str(dag)=="""Task(A=a1, B=b1)
Task(A=a1, B=b2)
Task(A=a2, B=b1)
Task(A=a2, B=b2)
Task(A=a1, C=c1)
Task(A=a1, C=c2)
Task(A=a2, C=c1)
Task(A=a2, C=c2)"""

    dag = DAG()
    A = dag.param("A", ["a1", "a2"])
    B = dag.param("B", ["b1", "b2"])
    C = dag.param("C", ["c1", "c2"])
    D = dag.param("D", ["d1", "d2"])
    A >> B
    A >> C
    B >> D
    C >> D
    assert str(dag)=='Task(A=a1, B=b1, D=d1)\nTask(A=a1, B=b1, D=d2)\nTask(A=a1, B=b2, D=d1)\nTask(A=a1, B=b2, D=d2)\nTask(A=a2, B=b1, D=d1)\nTask(A=a2, B=b1, D=d2)\nTask(A=a2, B=b2, D=d1)\nTask(A=a2, B=b2, D=d2)\nTask(A=a1, C=c1, D=d1)\nTask(A=a1, C=c1, D=d2)\nTask(A=a1, C=c2, D=d1)\nTask(A=a1, C=c2, D=d2)\nTask(A=a2, C=c1, D=d1)\nTask(A=a2, C=c1, D=d2)\nTask(A=a2, C=c2, D=d1)\nTask(A=a2, C=c2, D=d2)'



    dag = DAG()
    A = dag.param("A", ["a1", "a2"])
    B = dag.param("B", ["b1", "b2"])
    C = dag.param("C", ["c1", "c2"])
    A >> B
    B >> C
    assert str(dag)=='Task(A=a1, B=b1, C=c1)\nTask(A=a1, B=b1, C=c2)\nTask(A=a1, B=b2, C=c1)\nTask(A=a1, B=b2, C=c2)\nTask(A=a2, B=b1, C=c1)\nTask(A=a2, B=b1, C=c2)\nTask(A=a2, B=b2, C=c1)\nTask(A=a2, B=b2, C=c2)'



    dag = DAG()
    A = dag.param("A", ["a1", "a2"])
    B = dag.param("B", ["b1", "b2"])
    C = dag.param("C", ["c1", "c2"])
    D = dag.param("D", ["d1", "d2"])
    A >> B
    B >> C
    C >> D
    A >> D
    assert str(dag)=='Task(A=a1, B=b1, C=c1, D=d1)\nTask(A=a1, B=b1, C=c1, D=d2)\nTask(A=a1, B=b1, C=c2, D=d1)\nTask(A=a1, B=b1, C=c2, D=d2)\nTask(A=a1, B=b2, C=c1, D=d1)\nTask(A=a1, B=b2, C=c1, D=d2)\nTask(A=a1, B=b2, C=c2, D=d1)\nTask(A=a1, B=b2, C=c2, D=d2)\nTask(A=a2, B=b1, C=c1, D=d1)\nTask(A=a2, B=b1, C=c1, D=d2)\nTask(A=a2, B=b1, C=c2, D=d1)\nTask(A=a2, B=b1, C=c2, D=d2)\nTask(A=a2, B=b2, C=c1, D=d1)\nTask(A=a2, B=b2, C=c1, D=d2)\nTask(A=a2, B=b2, C=c2, D=d1)\nTask(A=a2, B=b2, C=c2, D=d2)\nTask(A=a1, D=d1)\nTask(A=a1, D=d2)\nTask(A=a2, D=d1)\nTask(A=a2, D=d2)'



    dag = DAG()
    A = dag.param("A", ["a1"])
    B = dag.param("B", ["b1"])
    C = dag.param("C", ["c1"])
    D = dag.param("D", ["d1"])
    A >> B
    A >> C
    B >> D
    C >> D
    A >> D
    assert str(dag)=='Task(A=a1, B=b1, D=d1)\nTask(A=a1, C=c1, D=d1)\nTask(A=a1, D=d1)'

