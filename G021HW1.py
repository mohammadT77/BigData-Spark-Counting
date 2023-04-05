from pyspark import SparkConf, SparkContext, RDD
from count_triangles import count_triangles
from argparse import ArgumentParser
from os.path import isfile
from random import randint
from time import time

def h_(c):
    p = 8191
    def hash_func(u):
        a = randint(1, p-1)
        b = randint(0, p-1)
        return ((a*u + b) % p) % c
    return hash_func

class Timer:
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

def MR_ApproxTCwithNodeColors(rdd:RDD, C:int) -> int:
    h_c = h_(C)

    # Evaluate hash colors
    colors_dict = rdd.flatMap(lambda x:x).distinct().map(lambda u: (u, h_c(u))).collectAsMap()

    def group_by_color(edge):
        e1, e2 = edge
        c1, c2 = colors_dict[e1], colors_dict[e2]
        return c1 if c1==c2 else -1
    
    t_final = (rdd.groupBy(group_by_color) # E(i) 
        .filter(lambda gp: gp[0]!=-1)  # ignore edges with different color of vertices
        .map(lambda gp: (gp[0], count_triangles(gp[1])))  # t(i)
        .values().sum() * C*C  # t_final
    )
    return t_final


def MR_ApproxTCwithSparkPartitions(rdd:RDD, C:int) -> int:
    # TODO: doesn't work well
    t_final = (
        rdd.mapPartitions(lambda edges: [count_triangles(edges)]) # t(i)
            .sum() # t_final
    )
    return t_final


if __name__ == '__main__':
    parser = ArgumentParser(description="BDC - Group 021 - Assignment 1")
    
    parser.add_argument('C', type=int, help='Number of colors')
    parser.add_argument('R', type=int, help='Number of Repetitions')
    parser.add_argument('path', metavar="FILE_PATH", type=str, help='Dataset file path')
    
    args = parser.parse_args()
    
    assert args.C >= 1, "Invalid argument C"
    assert args.R >= 1, "Invalid argument R"
    assert isfile(args.path), "Invalid data file path (argument FILE_PATH)"
    
    timer = Timer()

    # Spark configuration
    conf = SparkConf().setAppName("BDC: G021HW1")
    sc = SparkContext(conf=conf)

    # Reading dataset to RDD
    rdd = sc.textFile(args.path, minPartitions=args.C, use_unicode=False)
    rdd = rdd.map(lambda s: eval(b'('+s+b')')) # Convert edges from string to tuple.
    
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
        



