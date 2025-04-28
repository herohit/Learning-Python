import csv , os , argparse ,psutil,time,polars as pl
from argparse import Namespace


def process_csv(filename):
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


        # total_sum = sum(df.select(pl.all().sum()).row(0))

        end_time = time.time()
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
    process_csv(args.f)

# OUTPUT
# Start Time        : Mon Apr 28 21:25:48 2025
# End Time          : Mon Apr 28 21:25:49 2025
# Time Spent        : 1.05 seconds
# File Size         : 500.00 MB
# Memory Used       : 1068.86 MB
# Total Sum of CSV  : 67233713511