from pyspark import SparkConf, SparkContext
from count_triangles import count_triangles
from argparse import ArgumentParser

def MR_ApproxTCwithNodeColors(rdd, C) -> int:
    pass


def MR_ApproxTCwithSparkPartitions(rdd, C) -> int:
    pass


if __name__ == '__main__':
    parser = ArgumentParser(description="BDC - Group 021 - Assignment 1")
    
    parser.add_argument('C', type=int, help='Number of colors')
    parser.add_argument('R', type=int, help='Number of Repetitions')
    parser.add_argument('FILE', type=str, help='File path')
    parser.add_argument('-o','--output', required=False, default=None, help='path to save the output there (optional)')
    
    args = parser.parse_args()

    
