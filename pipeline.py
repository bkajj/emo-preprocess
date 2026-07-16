import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--config', default='configs/sample.yaml')
parser.add_argument('-s', '--samples', type=int, default=None)
args = parser.parse_args()

os.environ['PIPELINE_CONFIG_PATH'] = args.config

from config import config, EXTRACTED_PATH
from emo_datasets import *
from regression import evaluate_model_subject_independent, evaluate_model_subject_dependent
import pandas as pd 
import warnings;
from concurrent.futures import ProcessPoolExecutor
warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

DATASETS = {
    'biraffe': Biraffe,
    'case': Case,
    'deap': Deap
}

def run_dataset(name, window_time, sample_size=None, thread_num=0, max_threads=1):
    dataset = DATASETS[name]()
    return dataset.run(sample_size, thread_num, max_threads, window_time)

if __name__ == '__main__':
    jobs = []
    for name, cfg in config['datasets'].items(): # create list of jobs like (name, thread_num, max_threads)
        if cfg['enabled']:
            for thread_num in range(cfg['threads']):
                jobs.append((name, thread_num, cfg['threads']))

    with ProcessPoolExecutor(max_workers=len(jobs)) as executor:
        futures = {
            executor.submit(
                run_dataset, 
                name,
                config['datasets'][name]['window_time'],
                args.samples, 
                thread_num, 
                max_threads
            ): (name, thread_num) for name, thread_num, max_threads in jobs
        }

    results = {name: [] for name, cfg in config['datasets'].items() if cfg['enabled']}
    errors = {name: [] for name, cfg in config['datasets'].items() if cfg['enabled']}

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

    results_subject_dependent = evaluate_model_subject_dependent(results)
    print(results_subject_dependent)
    
    results_subject_independent = evaluate_model_subject_independent(results)
    print(results_subject_independent)


    results_subject_dependent.to_csv(os.path.join(EXTRACTED_PATH, 'results_sub_dep.csv'), index=False)
    results_subject_independent.to_csv(os.path.join(EXTRACTED_PATH, 'results_sub_indep.csv'), index=False)

    errors_merged = pd.concat(errors.values(), ignore_index=True)
    errors_merged.to_csv(os.path.join(EXTRACTED_PATH, 'errors.csv'), index=False)
