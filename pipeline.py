import os 
import yaml
import pandas as pd 
import argparse
from emo_datasets import *
import warnings;
from config import *
from concurrent.futures import ProcessPoolExecutor
from benchmark import benchmark_datasets
warnings.filterwarnings('ignore')

parser = argparse.ArgumentParser()
parser.add_argument('-s', '--samples', type=int, default=None)
parser.add_argument('-c', '--config', default=os.path.join(CONFIG_PATH, 'sample.yaml'))
args = parser.parse_args()

def run_dataset(name, window_time, sample_size=None, thread_num=0, max_threads=1):
    dataset = DATASETS[name]()
    return dataset.run(sample_size, thread_num, max_threads, window_time)

if __name__ == '__main__':
    with open('configs/sample.yaml') as f:
        config = yaml.safe_load(f)
        ds_configs = config['datasets']

    os.environ['USE_DATASET_MEMORY'] = '1' if config['cache']['raw_signals'] else '0'
    os.environ['USE_FEATURE_MEMORY'] = '1' if config['cache']['features'] else '0'

    jobs = []
    for name, cfg in ds_configs.items(): # create list of jobs like (name, thread_num, max_threads)
        if cfg['enabled']:
            for thread_num in range(cfg['threads']):
                jobs.append((name, thread_num, cfg['threads']))

    with ProcessPoolExecutor(max_workers=len(jobs)) as executor:
        futures = {
            executor.submit(
                run_dataset, 
                name,
                ds_configs[name]['window_time'],
                args.samples, 
                thread_num, 
                max_threads
            ): (name, thread_num) for name, thread_num, max_threads in jobs
        }

    results = {name: [] for name, cfg in ds_configs.items() if cfg['enabled']}
    errors = {name: [] for name, cfg in ds_configs.items() if cfg['enabled']}

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
            DATASETS[name]().merge_subjects_to_csv()

    benchmark_results = benchmark_datasets(results)
    print(benchmark_results)
    benchmark_results.to_csv(os.path.join(EXTRACTED_PATH, 'results.csv'), index=False)

    errors_merged = pd.concat(errors.values(), ignore_index=True)
    errors_merged.to_csv(os.path.join(EXTRACTED_PATH, 'errors.csv'), index=False)
