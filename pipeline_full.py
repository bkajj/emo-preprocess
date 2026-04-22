import pandas as pd
import os
import pickle
import warnings;
from preprocess import preprocess_signals, preprocess_deap
from config import *
import argparse
from multiprocessing import Process
warnings.filterwarnings('ignore')

def load_biraffe(sub_id):
    biosigs = pd.read_csv(os.path.join(BIRAFFE_PATH, f'{sub_id}-BioSigs.csv'), sep=',')
    annotations = pd.read_csv(os.path.join(BIRAFFE_ANNOTATIONS_PATH, f'{sub_id}-Procedure.csv'), sep=';')
    annotations = annotations.rename(columns={'ANS-VALENCE':'VALENCE', 'ANS-AROUSAL':'AROUSAL'})
    return biosigs, annotations

def load_case(sub_id):
    biosigs = pd.read_csv(os.path.join(CASE_PATH, f'{sub_id}.csv'), sep=',')
    biosigs = biosigs[['daqtime', 'ecg', 'gsr', 'video']].rename(columns={'gsr': 'EDA', 'ecg': 'ECG', 'daqtime':'TIMESTAMP', 'video':'VIDEO_ID'})
    
    annotations = pd.read_csv(os.path.join(CASE_ANNOTATIONS_PATH, f'{sub_id}.csv'), sep=',')
    annotations = annotations.rename(columns={'jstime': 'TIMESTAMP', 'valence': 'VALENCE', 'arousal': 'AROUSAL', 'video':'VIDEO_ID'})
    return biosigs, annotations

def load_deap(sub_id):
    with open(os.path.join(DEAP_PATH, f'{sub_id}.dat'), 'rb') as f:
        deap = pickle.load(f, encoding='latin1')

    annotations = pd.DataFrame(deap['labels'][:, :2], columns=['valence', 'arousal'])
    annotations['video_id'] = range(40)
    
    dfs = []
    deap_data = deap['data'][:, [36, 38], :]
    for i, video in enumerate(deap_data):
        df = pd.DataFrame(video.T, columns=['EDA', 'BVP'])
        df['video_id'] = i
        dfs.append(df)

    deap = pd.concat(dfs, ignore_index=True)
    return deap, annotations

def process_biraffe():
    filenames = [f for f in os.listdir(BIRAFFE_PATH) if f.endswith('.csv')]
    subjects = [f.split('-')[0] for f in filenames]
    
    os.makedirs(os.path.join(EXTRACTED_PATH, 'biraffe'), exist_ok=True)
    
    for s in subjects:
        print(f'Loading BIRAFFE subject: {s}', flush=True)
        biraffe, biraffe_ann = load_biraffe(s)
        result = preprocess_signals(data=biraffe, annotations=biraffe_ann, dataset_name='biraffe', subject_id=s)
        
def process_case():
    filenames = [f for f in os.listdir(CASE_PATH) if f.endswith('.csv')]
    subjects = [f.split('.')[0] for f in filenames]
    
    os.makedirs(os.path.join(EXTRACTED_PATH, 'case'), exist_ok=True)
    
    for s in subjects:
        print(f'Loading CASE subject: {s}', flush=True)
        case, case_ann = load_case(s)
        result = preprocess_signals(data=case, annotations=case_ann, dataset_name='case', subject_id=s)
        
def process_deap():
    filenames = [f for f in os.listdir(DEAP_PATH) if f.endswith('.dat')]
    subjects = [f.split('.')[0] for f in filenames]
    
    os.makedirs(os.path.join(EXTRACTED_PATH, 'deap'), exist_ok=True)
    
    for s in subjects:
        print(f'Loading DEAP subject: {s}', flush=True)
        deap, deap_ann = load_deap(s)
        result = preprocess_deap(data=deap, annotations=deap_ann, subject_id=s)

def process_dataset(dataset_name):
    con = {}
    if dataset_name == 'biraffe':
        fileformat = '.csv'
        splitchar = '-'
    if dataset_name == 'case':
        fileformat = '.csv'
        splitchar = '.'
    elif dataset_name == 'deap':
        fileformat = '.dat'
        splitchar = '.'
    else:
        return None

    filenames = [f for f in os.listdir(BIRAFFE_PATH) if f.endswith(fileformat)]
    subjects = [f.split(splitchar)[0] for f in filenames]

    for s in subjects:
        print(f'Loading DEAP subject: {s}', flush=True)
        deap, deap_ann = load_deap(s)
        result = preprocess_deap(data=deap, annotations=deap_ann, subject_id=s)


def merge_subjects_to_csv(dataset_name):
    dir = os.path.join(EXTRACTED_PATH, dataset_name)
    files = [os.path.join(dir, f) for f in os.listdir(dir)]
    dfs = [pd.read_csv(f) for f in files]
    
    result = pd.concat(dfs, ignore_index=True)
    final_filename = os.path.join(EXTRACTED_PATH, f'{dataset_name}.csv')
    result.to_csv(final_filename)
        
def run_pipeline():
    process_biraffe()
    process_case()
    process_deap()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--biraffe', action='store_true')
    parser.add_argument('-c', '--case', action='store_true')
    parser.add_argument('-d', '--deap', action='store_true')
    args = parser.parse_args()

    if not any([args.biraffe, args.case, args.deap]):
        args.biraffe = True
        args.case = True
        args.deap = True

    processes = []

    if args.biraffe:
        processes.append(Process(target=process_biraffe))
    if args.case:
        processes.append(Process(target=process_case))
    if args.deap:
        processes.append(Process(target=process_deap))
        
    for p in processes:
        p.start()
        
    for p in processes:
        p.join()
        
    for dataset_name in dataset_names:
        merge_subjects_to_csv(dataset_name)