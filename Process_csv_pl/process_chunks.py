import polars as pl
import time
import os
import psutil
import argparse
from argparse import Namespace

from utils import get_chunk_size


def chunk_csv_polar(filename):
    """
       Read a large CSV file in chunks and compute the total sum of all numeric values.
    """
    try:
        # start time
        start_time = time.time()

        # Track initial memory usage
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss

        # To calculate chunk size based on  memory available
        chunk_size = get_chunk_size(filename)

        total_sum = 0

        # Read Csv in batches
        reader = pl.read_csv_batched(filename, batch_size=chunk_size,has_header=False)

        while True:
            batch = reader.next_batches(1)
            if not batch:
                break  # If no more batches, break the loop

            # column wise sum of batch
            df_batch = batch[0].select(pl.all().sum())
            # get first row
            batch_row = df_batch.row(0)
            # total_sum += sum(batch[0].select(pl.all().sum()).row(0))
            total_sum += sum(batch_row)

        # End time
        end_time = time.time()
        # Track final memory usage
        mem_after = process.memory_info().rss

        print(f'Start Time        : {time.ctime(start_time)}')
        print(f'End Time          : {time.ctime(end_time)}')
        print(f'Time Spent        : {end_time - start_time:.2f} seconds')
        print(f'File Size         : {os.path.getsize(filename) / (1024 * 1024):.2f} MB')
        print(f'Memory Used       : {(mem_after - mem_before) / (1024 * 1024):.2f} MB')
        print(f'Total Sum of CSV  : {total_sum}')
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
    except pl.exceptions.PolarsError as e:
        print(f"Error reading CSV with Polars: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f',type=str,help="Give path of the file")
    args:Namespace = parser.parse_args()
    chunk_csv_polar(args.f)
# OUTPUT
# Start Time        : Mon Apr 28 21:25:33 2025
# End Time          : Mon Apr 28 21:25:36 2025
# Time Spent        : 3.33 seconds
# File Size         : 500.00 MB
# Memory Used       : 599.58 MB
# Total Sum of CSV  : 67233713511