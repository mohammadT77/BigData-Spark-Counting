from pyspark import SparkConf, SparkContext
from count_triangles import count_triangles
from argparse import ArgumentParser
from os.path import isfile

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
    
    # Spark
    conf = SparkConf().setAppName("BDC: G021HW1")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile(args.path, minPartitions=args.C, use_unicode=False)
    print("Dataset =", args.path)
    print("Number of Edges =", rdd.count())
    print("Number of Colors =", args.C)
    print("Number of Repetitions =", args.R)

    rdd = rdd.map(lambda s: eval(b'('+s+b')')) # Convert edges from string to tuple.

