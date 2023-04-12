from pyspark import SparkConf, SparkContext, RDD
from argparse import ArgumentParser
from os.path import isfile
from random import randint
from time import time
from collections import defaultdict
from statistics import median


def count_triangles(edges):
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


def string_to_tuple(line):
    vertices = line.split(',')
    return [(int(vertices[0]), int(vertices[1]))]    


def MR_ApproxTCwithNodeColors(rdd:RDD, C:int) -> int:
    p = 8191
    a = randint(1, p-1)
    b = randint(0, p-1)

    def keytozero(pair):
        return [(0, pair[1])]

    def H_C(edge):
        u, v = edge
        hash_u = ((a * u + b) % p) % C
        hash_v = ((a * v + b) % p) % C
        if hash_u == hash_v:
            return [(hash_u, edge)]
        else:
            return []    

    print("new repetition")

    start = time()
    triangle_count = (rdd.flatMap(H_C))
    end = time()
    print("runtime color:", (end-start)*1000)

    start = time()
    triangle_count = (triangle_count.groupByKey())
    end = time()
    print("runtime grouping:", (end-start)*1000)

    start = time()
    triangle_count = (triangle_count.mapValues(count_triangles))
    end = time()
    print("runtime count triangle:", (end-start)*1000)

    start = time()
    triangle_count = (triangle_count.flatMap(keytozero)
                .groupByKey()
                .mapValues(lambda count: sum(count) * C**2)) 
    end = time()
    print("runtime sum of partition:", (end-start)*1000)
    # triangle_count = (rdd.flatMap(H_C)
    #     .groupByKey()
    #     .mapValues(count_triangles)
    #     ).values().sum() * C **2
    return triangle_count


if __name__ == '__main__':
    # Configure argument parser
    parser = ArgumentParser(description="BDC - Group 021 - Assignment 1")
    
    parser.add_argument('C', type=int, help='Number of colors')
    parser.add_argument('R', type=int, help='Number of Repetitions')
    parser.add_argument('path', metavar="FILE_PATH", type=str, help='Dataset file path')
    
    args = parser.parse_args()
    
    # Validate arguments
    assert args.C >= 1, "Invalid argument C"
    assert args.R >= 1, "Invalid argument R"
    assert isfile(args.path), "Invalid data file path (argument FILE_PATH)"

    # Spark configuration
    conf = SparkConf().setAppName("BDC: G021HW1")
    sc = SparkContext(conf=conf)

    # Reading dataset to RDD
    rawData = sc.textFile(args.path)
    rawData = rawData.repartition(args.C)

    print("Dataset =", args.path)
    print("Number of Edges =", rawData.count())
    print("Number of Colors =", args.C)
    print("Number of Repetitions =", args.R)

    edges = rawData.flatMap(string_to_tuple).cache()

    print("Approximation through node coloring")
    t_final_list = []
    sum_time = 0
    for r in range(args.R):
        start = time()
        t_final = MR_ApproxTCwithNodeColors(edges, args.C)
        end = time()
        sum_time += (end - start)
        t_final_list.append(t_final)
        print(t_final.values())
    # print(f"- Number of triangles (median over {args.R} runs) = {median(t_final_list)}")
    print(f"- Running time (average over {args.R} runs) = {int(sum_time*1000)/args.R} ms")
    
        