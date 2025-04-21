import csv , os , argparse ,time, psutil ,polars as pl
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor

def sum_chunk(chunk):
    total_sum = chunk.select(pl.all().sum()).row(0)
    return sum(total_sum)
    # flat = [int(cell) for row in chunk for cell in row]
    # return sum(flat)

def process_csv_in_chunks_multithreaded(filename,thread,chunk_size):
    start_time = time.time()
    # memory
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss

    # total_sum = 0
    # with open(filename,'r',newline='') as csvfile:
    #     reader = csv.reader(csvfile)
    #     batch = []
    #     for row in reader:
    #         batch.append(row)
    #         if len(batch) == chunk_size * thread:
    #             # Split batch into `thread` number of chunks
    #             file_in_chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
    #             with ThreadPoolExecutor(max_workers=thread) as executor:
    #                 result = executor.map(sum_chunk, file_in_chunks)
    #                 total_sum += sum(result)
    #             batch = []  # clear memory for next batch
    #
    #     # handle remaining rows
    #     if batch:
    #         file_in_chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
    #         with ThreadPoolExecutor(max_workers=thread) as executor:
    #             result = executor.map(sum_chunk, file_in_chunks)
    #             total_sum += sum(result)


    # using polar
    lazy_df = pl.scan_csv(filename,has_header=False)
    total_rows = lazy_df.select(pl.len()).collect().item()

    # chunks = [lazy_df.slice(start,chunk_size).collect() for start in range(0,total_rows,chunk_size)]
    chunks = [lazy_df.slice(start, chunk_size) for start in range(0, total_rows, chunk_size)]
    #
    total_sum = 0
    with ThreadPoolExecutor(max_workers=thread) as executor:
        # using map
        result = list(executor.map(lambda chunk: sum_chunk(chunk.collect()), chunks))
        # result = list(executor.map(sum_chunk, chunks))
        total_sum = sum(result)

        # using submit
        # futures = []
        # for chunk in chunks:
        #     future = executor.submit(sum_chunk, chunk)
        #     futures.append(future)
        #
        # for future in futures:
        #     total_sum += future.result()


    end_time = time.time()
    mem_after = process.memory_info().rss

    print(f'Start Time        : {time.ctime(start_time)}')
    print(f'End Time          : {time.ctime(end_time)}')
    print(f'Time Spent        : {end_time - start_time:.2f} seconds')
    print(f'File Size         : {os.path.getsize(filename) / (1024 * 1024):.2f} MB')
    print(f'Memory Used       : {(mem_after - mem_before) / (1024 * 1024):.2f} MB')
    print(f'Total Sum of CSV  : {total_sum}')

if __name__ == '__main__':
    thread = psutil.cpu_count(logical=False)
    parser = argparse.ArgumentParser(description='Process csv')
    parser.add_argument('-f',type=str,help="File name of csv")
    parser.add_argument('-t',type=int,default=thread)
    parser.add_argument('-c', type=int, help="Give chunk size",default=50000)
    args = parser.parse_args()

    if args.t > thread:
        process_csv_in_chunks_multithreaded(args.f,thread,args.c)
    else:
        process_csv_in_chunks_multithreaded(args.f,args.t,args.c)
