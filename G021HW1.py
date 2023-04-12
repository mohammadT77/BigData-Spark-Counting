from os.path import isfile
from random import randint, random
from argparse import ArgumentParser
from time import time
from pyspark import SparkConf, SparkContext, RDD
from pyspark.storagelevel import StorageLevel

# from count_triangles import count_triangles
from collections import defaultdict

def count_triangles(edges) -> int:
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


def h_(c: int):
    """
    Returns a hash function with specified parameter C
    
    Examples
        --------
        >>> C = 4
        >>> h_c = h_(C)
        >>> h_c(1234)
        3
        >>> h_c(4321)
        1
    """
    p = 8191
    a = randint(1, p-1)
    b = randint(0, p-1)

    def hash_func(u: int) -> int:
        return ((a*u + b) % p) % c
    
    return hash_func

class Timer:
    """
        Provide a class to calculate elapsed time of inside a context.

        Examples
        --------
        >>> timer = Timer()
        >>> with timer:
        >>>     sleep(2)
        >>>     timer.elapsed_time()
        2.0001
        """
    def __init__(self, name=""):
        self.name = name
        self.reset()

    def __enter__(self):
        self.__start_t = time()
        self.__end_t = None
        return self

    def __exit__(self, *args):
        self.__end_t = time()

    def elapsed_time(self) -> float:
        assert self.__start_t, "Timer has NOT started yet."
        return (self.__end_t or time()) - self.__start_t
    
    def reset(self):
        self.__start_t = self.__end_t = None

    def __str__(self) -> str:
        return (self.name+" = " if self.name else '')+f"{int(self.elapsed_time()*1000)} ms"


def MR_ApproxTCwithNodeColors(rdd: RDD, C: int) -> int:
    # set h_c as the proper coloring hash function with C num of colors
    h_c = h_(C)

    def group_by_color(edge):
        """
        Returns the color of edge pairs if being in the same color, otherwise -1
        """
        v1, v2 = edge
        c1, c2 = h_c(v1), h_c(v2)
        return [(c1, (v1, v2))] if c1==c2 else []
    
    def f(a,b):
        a = a[1] if isinstance(a, tuple) else a
        b = b[1] if isinstance(b, tuple) else b
        return a+b
    # (Pay attention to the comments next to each line below)
    t_final = (
        rdd .flatMap(group_by_color)  # E(i) 
            .groupByKey()
            .map(lambda group: (group[0], count_triangles(group[1])))  # t(i)
            .reduce(f) * C**2  # t_final
    )
    return t_final if C > 1 else t_final[1]


def MR_ApproxTCwithSparkPartitions(rdd:RDD, C:int) -> int:
    # (Pay attention to the comments next to each line below)
    t_final = ( 
        rdd .mapPartitions(lambda edges: (yield count_triangles(edges)))  # t(i)
            .sum() * C**2  # t_final
    )
    return t_final


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

    # Timer instance for future uses
    timer = Timer()

    # Spark configuration
    conf = SparkConf().setAppName("BDC: G021HW1")
    sc = SparkContext(conf=conf)

    # Reading dataset to RDD
    rdd = sc.textFile(args.path, minPartitions=args.C, use_unicode=False)
    rdd = rdd.map(lambda s: eval(b'('+s+b')')) # Convert edges from string to tuple.
    rdd = rdd.partitionBy(args.C, lambda t:int(t*random()*10)%args.C) # Randomly partitioned
    rdd = rdd.cache()

    print("Dataset =", args.path)
    print("Number of Edges =", rdd.count())
    print("Number of Colors =", args.C)
    print("Number of Repetitions =", args.R)
    
    print("Approximation through node coloring")
    total_t_final = 0
    total_time = 0
    for r in range(args.R):
        with timer:
            t_final = MR_ApproxTCwithNodeColors(rdd, args.C)
            total_time += timer.elapsed_time()
            total_t_final += t_final
    print(f"- Number of triangles (median over {args.R} runs) = {total_t_final//args.R}")
    print(f"- Running time (average over {args.R} runs) = {int(total_time*1000)//args.R} ms")
    
    print("Approximation through Spark partitions")
    
    with timer:
        t_final = MR_ApproxTCwithSparkPartitions(rdd, args.C)
        total_time = timer.elapsed_time()
        print(f"- Number of triangles = {t_final}")
        print(f"- Running time = {int(total_time*1000)} ms")

