from concurrent.futures import ThreadPoolExecutor, as_completed
import polars as pl
import time
import os
import psutil
import  argparse
from argparse import Namespace

from utils import get_chunk_size



def process_batch(batch_list):
    """
       Processes a batch of data and computes the total sum of all columns.

       Args:
           batch_list (pl.DataFrame): A batch of data read from the CSV file.

       Returns:
           int: The total sum of all numeric columns in the batch.
       """
    # sum of all columns in batch
    batch_col_sum = batch_list.select(pl.all().sum())
    # Get First row containing individually sum of columns
    sum_col = batch_col_sum.row(0)
    # Total sum
    batch_sum = sum(sum_col)
    return batch_sum


def multithreaded_csv_polar(filename):
    """
    Processes a large CSV file using multiple threads to compute the sum of all numeric values.
    Tracks memory usage, execution time, and handles errors like missing files or reading issues.

    Args:
        filename (str) -f : The path to the CSV file to process.

    Raises:
        FileNotFoundError: If the CSV file is not found.
        pl.exceptions.PolarsError: If there's an issue with reading the CSV using Polars.
    Returns:
        Result : A dictionary containing the total sum of all numeric columns , start time , end time, time spent , file size and memory used.
    """
    try:
        # Track start time and memory
        start_time = time.time()
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss

        # get threads as per system resources
        total_num_threads =  psutil.cpu_count(logical=False) # logically false = CPU cores

        # Use 60% of total threads available
        num_threads = int(total_num_threads * 0.6)

        # Get batch size based on system memory
        batch_size = get_chunk_size(filename, num_threads)
        # read csv file
        reader = pl.read_csv_batched(filename, batch_size=batch_size,has_header=False)
        futures = []
        total_sum = 0

        # Thread pool executor for parallel processing
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            while True:
                batch_list = reader.next_batches(num_threads)
                if not batch_list:
                    break  # Exit if no more batches
                # Submit each batch to the thread pool for processing
                for batch in batch_list:
                    futures.append(executor.submit(process_batch, batch))
                # Process completed batches as they finish
            for f in as_completed(futures):
                total_sum += f.result()

        # Track End time and Memory
        end_time = time.time()
        mem_after = process.memory_info().rss

        # Time spent by the process
        time_spent = end_time - start_time

        # file size
        file_size = os.path.getsize(filename) / (1024 * 1024)

        # memory used
        mem_used=(mem_after - mem_before) / (1024 * 1024)

        # Store all the info in result
        result = {
            "start_time":time.ctime(start_time) ,
            "end_time" :time.ctime(end_time) ,
            "time_spent" : time_spent ,
            "file_size" : file_size ,
            "mem_used" :mem_used ,
            "total_sum" : total_sum

        }

        return result

    except FileNotFoundError:
        # Handle case when the file is not found
        print(f"Error: File '{filename}' not found.")
        raise
    except pl.exceptions.PolarsError as e:
        # Handle errors raised by Polars when reading the CSV
        print(f"Error reading CSV with Polars: {e}")
        raise
    except Exception as e:
        # Catch any unexpected exceptions
        print(f"An unexpected error occurred: {e}")
        raise


if __name__ == '__main__':
    # Set up command line argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-f',type=str,help="Give path of the file")

    # Parse the command-line arguments
    args:Namespace = parser.parse_args()

    # Call the CSV process function with parsed arguments
    data = multithreaded_csv_polar(args.f)

    # Print Result
    print(f'Start Time        : {data["start_time"]}')
    print(f'End Time          : {data["end_time"]}')
    print(f'Time Spent        : {data["time_spent"]:.2f} seconds')
    print(f'File Size         : {data["file_size"]:.2f} MB')
    print(f'Memory Used       : {data["mem_used"]:.2f} MB')
    print(f'Total Sum of CSV  : {data["total_sum"]}')

# OUTPUT
# Start Time        : Mon Apr 28 21:25:40 2025
# End Time          : Mon Apr 28 21:25:42 2025
# Time Spent        : 1.31 seconds
# File Size         : 500.00 MB
# Memory Used       : 631.32 MB
# Total Sum of CSV  : 67233713511