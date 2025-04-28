import polars as pl
import psutil
from numpy.f2py.auxfuncs import throw_error


def estimate_row_size(filepath, sample_row=100):
    """
        Estimate the average size of a single row in the CSV file.
    """
    # Read csv file
    df = pl.read_csv(filepath, n_rows=sample_row)
    # Get size of sample rows in bytes
    size_bytes = df.estimated_size()

    # Get size of one row and return
    row_size = size_bytes // sample_row
    return row_size


def get_chunk_size(filepath, num_threads=1, memory_fraction= 0.8):
    """
    Calculate chunk size (number of rows) for processing CSV files.
    """
    try:
        # Estimate size of one row
        row_size = estimate_row_size(filepath)

        # Get available system memory
        available_memory = psutil.virtual_memory().available

        # Use only a fraction of the available memory
        usable_memory = int(available_memory * memory_fraction)

        # Divide memory by number of threads
        memory_per_thread = usable_memory // num_threads

        # Calculate how many rows fit into memory per thread
        chunk_rows = memory_per_thread // row_size

        # Keep chunk size reasonable
        return max(10_000, min(chunk_rows, 100_000)) # max is 100,000 rows and min is 10,000 rows
    except Exception as e:
        print(f"Error calculating chunk size: {e}")
        raise