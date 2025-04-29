import csv
import random
import argparse
import os
import datetime
from argparse import Namespace


def generate_random_row(column):
    """
       Generate a list of random integers between 1 and 1000.

       Args:
           column (int): Number of columns in the row.

       Returns:
           list: A list containing `column` number of random integers.
       """
    row = [random.randint(1,1000) for i in range(column)]
    return row


def generate_csv(size=None,rows=None,columns=100):
    """
        Generate a CSV file with random integer data.

        The file can be generated based on the number of rows or the total file size in MB.

        Args:
            size (int) -s: Target size of the CSV file in megabytes. Default is None.
            rows (int) -r : Number of rows to generate. Default is None.
            columns (int, optional): Number of columns in each row. Default is 100.

        Returns:
            None
        Raises:
           PermissionError:If there are permission issues
        """
    if rows is None and size is None:
        print('Please provide size or rows')
        return

    # Generate a unique filename using the current timestamp : YYYYMMDD_HHMMSS e.g. 20250421_212115.csv
    name = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'{name}.csv'
    try:
    # Open the CSV file for writing
        with open(filename,'w',newline="") as csvfile:
            writer = csv.writer(csvfile)

            if rows:
                # Generate a fixed number of rows
                for _ in range(rows):
                    row = generate_random_row(columns)
                    writer.writerow(row)

            elif size:
                # Generate rows until the file reaches the desired size in MB
                target_size = size * 1024 * 1024
                while os.path.getsize(filename) < target_size:
                    row = generate_random_row(columns)
                    writer.writerow(row)
            print(f'Generated {name} successfully')

    except PermissionError:
        # Handle cases where there are permission issues
        print("Error: You do not have permission to write to the current directory.")
        raise
    except Exception as e:
        # Catch any unexpected exceptions
        print(f"An unexpected error occurred: {e}")
        raise


if __name__ == "__main__":

    # Set up argument parsing for command-line usage
    parser = argparse.ArgumentParser(description="Generate csv file")
    parser.add_argument('-s',type=int,help='Size of csv file')
    parser.add_argument('-r',type=int,help='rows in csv file')

    # Parse the command-line arguments
    args:Namespace = parser.parse_args()

    # Call the CSV generation function with parsed arguments
    generate_csv(args.s,args.r)


