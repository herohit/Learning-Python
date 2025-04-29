import os
import argparse
import psutil
import time
import polars as pl
from argparse import Namespace


def process_csv(filename):
    """
       Processes a CSV file by reading it using Polars, calculating the sum of all columns,
       and printing relevant information about time spent, memory usage, and the total sum.

       Args:
           filename (str) -f : The path to the CSV file to process.

       Raises:
           FileNotFoundError: If the CSV file cannot be found.
           pl.exceptions.PolarsError: If there's an error reading the CSV with Polars.
       """
    try:
        start_time = time.time()

        # track memory
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss

        # Read CSV file using polars
        df = pl.read_csv(filename,has_header=False)

        # Select all columns and compute their sum individually
        sum_df = df.select(
            pl.all().sum()
        )

        # Get the first row
        sum_row = sum_df.row(0)

        # Sum all the rows
        total_sum = sum(sum_row)

        end_time = time.time()
        mem_after = process.memory_info().rss

        print(f'Start Time        : {time.ctime(start_time)}')
        print(f'End Time          : {time.ctime(end_time)}')
        print(f'Time Spent        : {end_time - start_time:.2f} seconds')
        print(f'File Size         : {os.path.getsize(filename) / (1024 * 1024):.2f} MB')
        print(f'Memory Used       : {(mem_after - mem_before) / (1024 * 1024):.2f} MB')
        print(f'Total Sum of CSV  : {total_sum}')


    except FileNotFoundError:
        # Handle case when the file is not found
        print(f"Error: File '{filename}' not found.")
        raise
    except pl.exceptions.PolarsError as e:
        # Handle errors raised by Polars when reading the CSV
        print(f"Error reading CSV with Polars: {e}")
        raise
    except PermissionError:
        # Handle cases where there are permission issues
        print(f"Error: Permission denied while accessing '{filename}'.")
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
    process_csv(args.f)

# OUTPUT
# Start Time        : Mon Apr 28 21:25:48 2025
# End Time          : Mon Apr 28 21:25:49 2025
# Time Spent        : 1.05 seconds
# File Size         : 500.00 MB
# Memory Used       : 1068.86 MB
# Total Sum of CSV  : 67233713511