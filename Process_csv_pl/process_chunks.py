import polars as pl, time, os, psutil,argparse
from argparse import Namespace


def estimate_row_size(filepath, sample_row=100):
    df = pl.read_csv(filepath, n_rows=sample_row)
    size_bytes = df.estimated_size()
    row_size = size_bytes // sample_row
    return row_size


def get_chunk_size(filepath, memory_fraction=0.8):
    row_size = estimate_row_size(filepath)
    available_memory = psutil.virtual_memory().available
    usable_memory = int(available_memory * memory_fraction)
    chunk_rows = usable_memory // row_size
    return max(10_000, min(chunk_rows, 1_000_00))


def chunk_csv_polar(filename):
    start_time = time.time()

    # track memory
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss


    chunk_size = get_chunk_size(filename)

    total_sum = 0

    reader = pl.read_csv_batched(filename, batch_size=chunk_size)

    while True:
        batch = reader.next_batches(1)
        if not batch:
            break
        total_sum += sum(batch[0].select(pl.all().sum()).row(0))

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
    chunk_csv_polar(args.f)