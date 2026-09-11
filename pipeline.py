import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--config', default='configs/sample.yaml')
parser.add_argument('-s', '--samples', type=int, default=None)
args = parser.parse_args()

os.environ['PIPELINE_CONFIG_PATH'] = args.config

from datetime import datetime
os.environ['PIPELINE_RUN_ID'] = datetime.now().strftime('%y-%m-%d_%H%M')

from config import config, RUN_RESULT_PATH
from emo_datasets import *
from regression import evaluate_model_subject_independent, evaluate_model_subject_dependent
import pandas as pd 
import warnings
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
    failed_subs = {name: [] for name, cfg in config['datasets'].items() if cfg['enabled']}

    for future in futures:
        name, thread_num = futures[future]
        r, e, f = future.result()
        results[name].append(r)
        errors[name].append(e)
        failed_subs[name].append(f)

    for name in results:
        results[name] = pd.concat(results[name], ignore_index=True)
        errors[name] = pd.concat(errors[name], ignore_index=True)
        failed_subs[name] = [s for lista in failed_subs[name] for s in lista]

    if args.samples is None:
        for name in results:
            DATASETS[name]().merge_subjects_to_csv()

    metrics_sub_dep = evaluate_model_subject_dependent(results)
    metrics_sub_dep.reset_index().rename(columns={'index': 'dataset'})
    print("SUBJECT DEPENDENT - RESULTS")
    print(metrics_sub_dep)

    metrics_sub_indep, fa_sub_indep = evaluate_model_subject_independent(results)
    print("SUBJECT INDEPENDENT - RESULTS")
    print(metrics_sub_indep)
    print(fa_sub_indep)

    metrics_sub_dep.to_csv(os.path.join(RUN_RESULT_PATH, 'results_sub_dep.csv'), index=True, float_format='%.4f')
    metrics_sub_indep.to_csv(os.path.join(RUN_RESULT_PATH, 'results_sub_indep.csv'), index=False, float_format='%.4f')
    fa_sub_indep.to_csv(os.path.join(RUN_RESULT_PATH, 'feature_analysis_sub_indep.csv'), index=False, float_format='%.4f')

    errors_merged = pd.concat(errors.values(), ignore_index=True)
    errors_merged.to_csv(os.path.join(RUN_RESULT_PATH, 'errors.csv'), index=False)

    for name, subs in failed_subs.items():
        print(f'[{name}] przetworzono: {results[name].SUBJECT_ID.nunique()}, nieudanych: {len(subs)}', end='')
        print(f' -> {subs}' if subs else '')

    rows = [{'dataset': name, 'subject_id': s} for name, subs in failed_subs.items() for s in subs]
    pd.DataFrame(rows, columns=['dataset', 'subject_id']).to_csv(os.path.join(RUN_RESULT_PATH, 'failed_subs.csv'), index=False)
