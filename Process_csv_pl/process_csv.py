import csv , os , argparse ,psutil,time,polars as pl
from argparse import Namespace


def process_csv(filename):
    start_time = time.time()

    # track memory
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss

    df = pl.read_csv(filename)
    total_sum = sum(df.select(pl.all().sum()).row(0))

    end_time = time.time()
    mem_after = process.memory_info().rss

    print(f'Start Time        : {time.ctime(start_time)}')
    print(f'End Time          : {time.ctime(end_time)}')
    print(f'Time Spent        : {end_time - start_time:.2f} seconds')
    print(f'File Size         : {os.path.getsize(filename) / (1024 * 1024):.2f} MB')
    print(f'Memory Used       : {(mem_after - mem_before) / (1024 * 1024):.2f} MB')
    print(f'Total Sum of CSV  : {total_sum}')




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f',type=str,help="Give path of the file")
    args:Namespace = parser.parse_args()
    process_csv(args.f)