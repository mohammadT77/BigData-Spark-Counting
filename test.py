from G021HW1 import *


timer = Timer()


def test(func, rdd, C, R):
    total_t_final = 0
    total_time = 0
    for r in range(R):
        with timer:
            t_final = func(rdd, C)
            total_time += timer.elapsed_time()
            total_t_final += t_final
    
    return total_t_final//R, int(total_time*1000)//R

if __name__ == '__main__':
    file_path = "data/facebook_small.txt"
    
    conf = SparkConf().setAppName("BDC: G021HW1 - TEST")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile(file_path, minPartitions=2, use_unicode=False).cache()
    rdd = rdd.map(lambda s: eval(b'('+s+b')')) # Convert edges from string to tuple.

    res = []
    for f in (MR_ApproxTCwithNodeColors, MR_ApproxTCwithSparkPartitions):
        res.append([(f.__name__,f.__name__)]+[test(f, rdd, c, r) for c,r in [(1,1), (2,5), (4,5), (8,5)]])

    from rich.console import Console
    from rich.table import Table
    print("FILE:", file_path)
    num_table = Table(title="Numbers")
    columns = ["ALGO","C1R1", "C2R5", "C4R5", "C8R5"]

    for column in columns:
        num_table.add_column(column)

    for row in res:
        num_table.add_row(*map(lambda t: str(t[0]), row), style='bright_green')

    console = Console()
    console.print(num_table)

    num_table = Table(title="Time (MS)")
    columns = ["ALGO", "C1R1", "C2R5", "C4R5", "C8R5"]

    for column in columns:
        num_table.add_column(column)

    for row in res:
        num_table.add_row(*map(lambda t: str(t[1]), row), style='bright_green')

    console = Console()
    console.print(num_table)
