import polars as pl
import time
import os
import psutil
import argparse
from argparse import Namespace

from utils import get_chunk_size


def chunk_csv_polar(filename):
    """
    Reads a large CSV file in chunks and computes the total sum of all numeric columns.
    It tracks execution time, memory usage, and handles errors like missing files and reading issues.

    Args:
        filename (str): The path to the CSV file to process.

    Raises:
        FileNotFoundError: If the CSV file is not found.
        pl.exceptions.PolarsError: If there's an issue with reading the CSV using Polars.

    Returns:
        Result : A dictionary containing the total sum of all numeric columns , start time , end time, time spent , file size and memory used.
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

            total_sum += sum(batch_row)

        # End time
        end_time = time.time()
        # Track final memory usage
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
    data = chunk_csv_polar(args.f)

    # Print Data
    print(f'Start Time        : {data["start_time"]}')
    print(f'End Time          : {data["end_time"]}')
    print(f'Time Spent        : {data["time_spent"]:.2f} seconds')
    print(f'File Size         : {data["file_size"]:.2f} MB')
    print(f'Memory Used       : {data["mem_used"]:.2f} MB')
    print(f'Total Sum of CSV  : {data["total_sum"]}')

# OUTPUT
# Start Time        : Mon Apr 28 21:25:33 2025
# End Time          : Mon Apr 28 21:25:36 2025
# Time Spent        : 3.33 seconds
# File Size         : 500.00 MB
# Memory Used       : 599.58 MB
# Total Sum of CSV  : 67233713511