from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor, as_completed
import polars as pl, time, os, psutil ,  argparse
from argparse import Namespace


def estimate_row_size(filepath, sample_row=100):
    df = pl.read_csv(filepath, n_rows=sample_row)
    size_bytes = df.estimated_size()
    row_size = size_bytes // sample_row
    return row_size


def get_chunk_size(filepath, num_threads,memory_fraction=0.8):
    row_size = estimate_row_size(filepath)
    available_memory = psutil.virtual_memory().available
    usable_memory = int(available_memory * memory_fraction)
    mem_per_thread = usable_memory // num_threads
    chunk_rows = mem_per_thread // row_size
    return max(5_000, min(chunk_rows, 100_000))


def process_batch(batch_list):
    return sum(batch_list.select(pl.all().sum()).row(0))


def multithreaded_csv_polar(filename):
    start_time = time.time()

    # track memory
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss


    num_threads = min(4, psutil.cpu_count(logical=False))
    batch_size = get_chunk_size(filename, num_threads)

    reader = pl.read_csv_batched(filename, batch_size=batch_size)

    futures = []
    total_sum = 0

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        while True:
            batch_list = reader.next_batches(num_threads)
            if not batch_list:
                break
            for batch in batch_list:
                futures.append(executor.submit(process_batch, batch))
            for f in as_completed(futures):
                total_sum += f.result()

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
    multithreaded_csv_polar(args.f)