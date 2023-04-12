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


def MR_ApproxTCwithSparkPartitions(edges,c):
    triangle_counting_random = (edges.groupBy(lambda x: (rand.randint(0,c-1)))#.map(lambda x : (x[0], list(x[1])))
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
    edges = rawData.flatMap(rawData_to_edges)
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

    for i in range(r):
        start_time = time()
        Number_of_triangles_spark.append(MR_ApproxTCwithSparkPartitions(edges,c))
        cur_runtime = (time() - start_time)*1000
        avg_running_time2+= cur_runtime
    avg_running_time2 = avg_running_time2/r
    # printing file information

    print("Dataset = "+data_path)
    print("Number of Edges = ")
    print("Number of Colors = "+str(c))
    print("Number of Repetitions = "+str(r))
    
    print("Number of Triangle in the graph =",statistics.median(Number_of_triangles))
    print("Runtime = ",avg_running_time1)


    print("Number of Triangle in the graph =", statistics.median(Number_of_triangles_spark))
    print("Runtime = ", avg_running_time2)
    #print("Number of Triangle in the graph =", Number_of_triangles_spark[0].collect())#[0].collect())
#setting global variables

if __name__ == '__main__':
   main()


