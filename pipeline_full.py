import argparse
import os 

parser = argparse.ArgumentParser()
parser.add_argument('-b', '--biraffe', action='store_true')
parser.add_argument('-c', '--case', action='store_true')
parser.add_argument('-d', '--deap', action='store_true')
parser.add_argument('-s', '--samples', type=int, default=None)
parser.add_argument('--no-cache', action='store_true') # don't use when processing data for all subjects
args = parser.parse_args()

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

def run_dataset(name, sample_size=None):
    dataset = datasets[name]()
    return dataset.run(sample_size)

if __name__ == '__main__':
    if not any([args.biraffe, args.case, args.deap]):
        args.biraffe = True
        args.case = True
        args.deap = True

    processes = []

    with ProcessPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(run_dataset, name, args.samples): name for name in datasets.keys()
        }

    results = {}
    for future in futures:
        name = futures[future]
        results[name] = future.result()

    benchmark_results = benchmark_datasets(results)
    print(benchmark_results)

    