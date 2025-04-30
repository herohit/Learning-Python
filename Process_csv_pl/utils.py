import polars as pl
import psutil


def estimate_row_size(filepath, sample_row=100):
    """
    Estimate the average size of a single row in a CSV file.
    This function reads a sample of rows from the file and calculates
    the estimated size of each row in bytes.

    Args:
        filepath (str): Path to the CSV file.
        sample_row (int, optional): The number of rows to sample for size estimation. Default is 100.

    Returns:
        int: The estimated size of a single row in bytes.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        polars.exceptions.PolarsError: If there is an issue reading the CSV.
        Exception: For any other unexpected errors.
    """
    try:
        # Read csv file
        df = pl.read_csv(filepath, n_rows=sample_row)
        # Get size of sample rows in bytes
        size_bytes = df.estimated_size()

        # Get size of one row and return
        row_size = size_bytes // sample_row
        return row_size
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found.")
        raise
    except pl.exceptions.PolarsError as e:
        print(f"Error reading CSV with Polars: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


def get_chunk_size(filepath, num_threads=1, memory_fraction= 0.6):
    """
    Calculate the chunk size (number of rows) for processing a CSV file based on available system memory.
    The chunk size is calculated to ensure that each thread processes a reasonable amount of data
    without exceeding the available memory.

    Args:
        filepath (str): Path to the CSV file.
        num_threads (int, optional): Number of threads to use for processing. Default is 1.
        memory_fraction (float, optional): The fraction of available memory to be used for processing. Default is 0.6 (60%).

    Returns:
        int: The calculated chunk size (number of rows).

    Raises:
        Exception: If an error occurs during the calculation.
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
        # Catch any unexpected exceptions
        print(f"Error calculating chunk size: {e}")
        raise