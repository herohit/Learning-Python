import csv
import random
import argparse
import os
import datetime
from argparse import Namespace

def generate_random_row(column):
    row = [random.randint(1,1000) for i in range(column)]
    return row


def generate_csv(size=None,rows=None,columns=100):
    if rows is None and size is None:
        print('Please provide size or rows')
        return


    name = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'{name}.csv'
    with open(filename,'w',newline="") as csvfile:
        writer = csv.writer(csvfile)

        if rows:
            for _ in range(rows):
                row = generate_random_row(columns)
                writer.writerow(row)

        elif size:
            target_size = size * 1024 * 1024
            while os.path.getsize(filename) < target_size:
                row = generate_random_row(columns)
                writer.writerow(row)
        print(f'Generated {name} successfully')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate csv file")
    parser.add_argument('-s',type=int,help='Size of csv file')
    parser.add_argument('-r',type=int,help='rows in csv file')

    args:Namespace = parser.parse_args()
    generate_csv(args.s,args.r)


