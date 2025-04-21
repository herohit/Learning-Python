import csv , os , argparse ,psutil,time,polars as pl
from argparse import Namespace


def sum_chunk(chunk):
    chunk_sum = 0
    for row in chunk:
        for value in row:
            chunk_sum += int(value)
    return chunk_sum


def process_csv_in_chunks(filename,chunk_size=100000):
    start_time = time.time()
    # memory
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss

    # raw python
    total_sum = 0
    row_count = 0
    with open(filename,'r',newline='') as csvfile:
        reader = csv.reader(csvfile)
        chunk = []

        for row in reader:
            row_count +=1
            chunk.append(row)
            if row_count == chunk_size:
                total_sum += sum_chunk(chunk)
                chunk = []
                row_count=0
        # left over rows in chunks
        if chunk:
            total_sum += sum_chunk(chunk)
            del chunk

            # with polar
    # lazy_df = pl.scan_csv(filename,has_header=False)
    # total_rows = lazy_df.select(pl.len()).collect().item()
    #
    # for start in range(0,total_rows,chunk_size):
    #     chunk = lazy_df.slice(start , chunk_size).collect()
    #     chunk_sum = sum(chunk.select(pl.all().sum()).row(0))
    #     total_sum += chunk_sum



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
    parser.add_argument('-c',type=int,help="Give chunk size")
    args:Namespace = parser.parse_args()
    if args.c:
        process_csv_in_chunks(args.f,args.c)
    else:
        process_csv_in_chunks(args.f)