from pyspark import SparkConf, SparkContext
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

def MR_ApproxTCwithNodeColors(rdd, C) -> int:
    pass


def MR_ApproxTCwithSparkPartitions(rdd, C) -> int:
    pass


if __name__ == '__main__':
    parser = ArgumentParser(description="BDC - Group 021 - Assignment 1")
    
    parser.add_argument('C', type=int, help='Number of colors')
    parser.add_argument('R', type=int, help='Number of Repetitions')
    parser.add_argument('path', metavar="FILE_PATH", type=str, help='Dataset file path')
    
    args = parser.parse_args()
    
    assert args.C >= 1, "Invalid argument C"
    assert args.R >= 1, "Invalid argument R"
    assert isfile(args.path), "Invalid data file path (argument FILE_PATH)"
    
    h_c = h_(args.C)

    # Spark
    conf = SparkConf().setAppName("BDC: G021HW1")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile(args.path, minPartitions=args.C, use_unicode=False)
    rdd = rdd.map(lambda s: eval(b'('+s+b')')) # Convert edges from string to tuple.


    # Evaluate hash colors
    colors_dict = rdd.flatMap().distinct().map(lambda u: (u, h_c(u))).collectAsMap()



    print("Dataset =", args.path)
    print("Number of Edges =", rdd.count())
    print("Number of Colors =", args.C)
    print("Number of Repetitions =", args.R)



