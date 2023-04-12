<<<<<<< HEAD:Triangle_Counting.py
from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand
from collections import defaultdict
from time import time
import statistics



def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def rawData_to_edges(rawData):
    vertexes = rawData.split(",", 1)
    v = int(vertexes[0])
    u = int(vertexes[1])
    return [[u,v]]

def MR_Approx_TCwithNodeColors(edges,c):
    p = 8191
    a = rand.randint(1, p - 1)
    b = rand.randint(0, p - 1)

    def hash_function(edge):
        v,u = edge

        hash_codeV1 = ((a * v + b) % p) % c
        hash_codeV2 = ((a * u + b) % p) % c
        if hash_codeV1 == hash_codeV2:
            return [(hash_codeV1, (v, u))]
        else:
            return []

    def keytozero(pair):
        pairs_dict = {}
        pairs_dict[0] = pair[1]
        return [(key, pairs_dict[key]) for key in pairs_dict.keys()]
    print(edges.flatMap(hash_function).collect())
    triangle_counting = (edges.flatMap(hash_function) # <-- MAP PHASE (0,(2000,2001))
                .groupByKey() # (0,[(2000,2001),(2009,2008),...]
                .mapValues(CountTriangles)  # (0,2200) , (1,2100) , (2,2000),3(
                .flatMap(keytozero)
                .groupByKey()
                .mapValues(lambda count: sum(count))
                )
    return triangle_counting






def main():
    assert len(sys.argv) == 4, "Usage: python Triangle_Counting.py <C> <R> <file_name>"
    conf = SparkConf().setAppName("Triangle_Counting")
    sc = SparkContext(conf=conf)

    #input reading

    #1. read number of partitions
    c = sys.argv[1]
    assert c.isdigit(), "k must be an integer"
    c = int(c)

    r = sys.argv[2]
    assert r.isdigit(), "k must be an integer"
    r = int(r)

    #2. read input file and subdivide it into k random partitions
    data_path = sys.argv[3]
    assert os.path.isfile(data_path), "File or folder not found"
    rawData = sc.textFile(data_path).repartition(numPartitions=c).cache()
    edges = rawData.flatMap(rawData_to_edges)
    #docs.repartition(numPartitions=c)

    #setting global variables

    Number_of_triangles = []
    avg_running_time = 0
    cur_runtime = 0
    for i in range(r):
        start_time = time()
        Number_of_triangles.append(MR_Approx_TCwithNodeColors(edges,c).collect()[0][1])
        cur_runtime = (time() - start_time)*1000
        avg_running_time+= cur_runtime
    avg_running_time = avg_running_time/r
    # printing file information
    print("Dataset = "+data_path)
    print("Number of Edges = ")
    print("Number of Colors = "+str(c))
    print("Number of Repetitions = "+str(r))

    print("Number of Triangle in the graph =",statistics.median(Number_of_triangles) *c*c)
    print("Runtime = ",avg_running_time)
    #print(docs.top(1))

#setting global variables

if __name__ == '__main__':
   main()


=======
from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand
from collections import defaultdict
from time import time
import statistics



def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def rawData_to_edges(rawData):
    vertexes = rawData.split(",", 1)
    v = int(vertexes[0])
    u = int(vertexes[1])
    return [[u,v]]

def MR_ApproxTCwithNodeColors(edges,c):
    p = 8191
    a = rand.randint(1, p - 1)
    b = rand.randint(0, p - 1)


    def hash_function(edge):
        v,u = edge

        hash_codeV1 = ((a * v + b) % p) % c
        hash_codeV2 = ((a * u + b) % p) % c
        if hash_codeV1 == hash_codeV2:
            return [(hash_codeV1,edge)]
        return []


    triangle_counting = (edges.flatMap(hash_function) # <-- MAP PHASE (0,(2000,2001))
                .groupByKey() # (0,[(2000,2001),(2009,2008),...]
                .mapValues(CountTriangles)  # (0,2200) , (1,2100) , (2,2000),3(
                .values()
                .sum()*c*c
                )
    return triangle_counting

key = -1
def MR_ApproxTCwithSparkPartitions(edges,c):

    def add_key(edges):
        global key
        key = key+1
        return [(key,edges)]
    triangle_counting_random = (edges.glom()
                         .flatMap(add_key)
                         .mapValues(CountTriangles)  # (0,2200) , (1,2100) , (2,2000),3(
                         .values()
                         .sum()*c*c
                         )
    return triangle_counting_random





def main():
    assert len(sys.argv) == 4, "Usage: python Triangle_Counting.py <C> <R> <file_name>"
    conf = SparkConf().setAppName("Triangle_Counting")
    sc = SparkContext(conf=conf)

    #input reading

    #1. read number of partitions
    c = sys.argv[1]
    assert c.isdigit(), "k must be an integer"
    c = int(c)

    r = sys.argv[2]
    assert r.isdigit(), "k must be an integer"
    r = int(r)

    #2. read input file and subdivide it into k random partitions
    data_path = sys.argv[3]
    assert os.path.isfile(data_path), "File or folder not found"
    rawData = sc.textFile(data_path).repartition(numPartitions=c).cache()
    edges = rawData.flatMap(rawData_to_edges).repartition(numPartitions=c).cache()
    #docs.repartition(numPartitions=c)

    #setting global variables

    Number_of_triangles = []
    Number_of_triangles_spark = []
    avg_running_time1 = 0
    avg_running_time2 = 0
    cur_runtime = 0

    for i in range(r):
        start_time = time()
        Number_of_triangles.append(MR_ApproxTCwithNodeColors(edges,c))
        cur_runtime = (time() - start_time)*1000
        avg_running_time1+= cur_runtime
    avg_running_time1 = avg_running_time1/r

    start_time = time()
    Number_of_triangles_spark = MR_ApproxTCwithSparkPartitions(edges,c)
    avg_running_time2 = (time() - start_time)*1000
    # printing file information

    print("Dataset = "+data_path)
    print("Number of Edges = ")
    print("Number of Colors = "+str(c))
    print("Number of Repetitions = "+str(r))

    print("Approximation through node coloring")
    print("- Number of Triangle (median over "+str(r)+ " runs) =",statistics.median(Number_of_triangles))
    print("- Running time (average over "+str(r)+" runs) = ",avg_running_time1)

    print("Approximation through spark partitions")
    print("- Number of Triangle =", Number_of_triangles_spark)
    print("- Running time = ", avg_running_time2)
    #print("Number of Triangle in the graph =", Number_of_triangles_spark[0].collect().__len__())#.collect())
#setting global variables

if __name__ == '__main__':
   main()


>>>>>>> d426cf3d295d1bc2dbf30f6862761ac72c2f9dfc:G021HW1_Ali.py