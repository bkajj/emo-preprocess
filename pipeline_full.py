import argparse
import os 
import pandas as pd 

def positive_int(value):
    ivalue = int(value)
    if ivalue < 1:
        raise argparse.ArgumentTypeError(f'{value} is not a positive integer')
    return ivalue

parser = argparse.ArgumentParser()
parser.add_argument('-b', '--biraffe', type=positive_int, default=None)
parser.add_argument('-c', '--case', type=positive_int, default=None)
parser.add_argument('-d', '--deap', type=positive_int, default=None)
parser.add_argument('-s', '--samples', type=positive_int, default=None)
parser.add_argument('--no-cache', action='store_true') # don't use when processing data for all subjects
args = parser.parse_args()

thread_counts = {
    'biraffe': 3 if args.biraffe is None else args.biraffe,
    'case': 1 if args.case is None else args.case,
    'deap': 1 if args.deap is None else args.deap
}

if args.no_cache:
    os.environ['USE_DATASET_MEMORY'] = '0'
else:
    os.environ['USE_DATASET_MEMORY'] = '1'

from emo_datasets import *
import warnings;
from config import *
from concurrent.futures import ProcessPoolExecutor
from benchmark import benchmark_datasets
warnings.filterwarnings('ignore')

datasets = {
    'biraffe': Biraffe,
    'case': Case,
    'deap': Deap
}

def run_dataset(name, sample_size=None, thread_num=0, max_threads=1):
    dataset = datasets[name]()
    return dataset.run(sample_size, thread_num, max_threads)

if __name__ == '__main__':
    jobs = []
    for name, max_threads in thread_counts.items(): # create list of jobs like (name, thread_num, max_threads)
        for thread_num in range(max_threads):
            jobs.append((name, thread_num, max_threads))

    with ProcessPoolExecutor(max_workers=len(jobs)) as executor:
        futures = {
            executor.submit( # submit all jobs from the list
                run_dataset, 
                name, 
                args.samples, 
                thread_num, 
                max_threads
            ): (name, thread_num) for name, thread_num, max_threads in jobs
        }

    results = {name: [] for name in thread_counts if thread_counts[name] > 0}
    errors = {name: [] for name in thread_counts if thread_counts[name] > 0}

    for future in futures:
        name, thread_num = futures[future]
        r, e = future.result()
        results[name].append(r)
        errors[name].append(e)

    for name in results:
        results[name] = pd.concat(results[name], ignore_index=True)
        errors[name] = pd.concat(errors[name], ignore_index=True)

    if args.samples is None:
        for name in results:
            datasets[name]().merge_subjects_to_csv()

    benchmark_results = benchmark_datasets(results)
    print(benchmark_results)

    errors_merged = pd.concat(errors.values(), ignore_index=True)
    errors_merged.to_csv(os.path.join(EXTRACTED_PATH, 'errors.csv'), index=False)
