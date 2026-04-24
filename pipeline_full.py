from emo_datasets import *
import warnings;
from config import *
import argparse
from multiprocessing import Process
from emo_datasets import *
warnings.filterwarnings('ignore')

datasets = {
    'biraffe': Biraffe,
    'case': Case,
    'deap': Deap
    }

def run_dataset(name):
    dataset = datasets[name]()
    dataset.run()

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
        processes.append(Process(target=run_dataset, args=('biraffe',)))
    if args.case:
        processes.append(Process(target=run_dataset, args=('case',)))
    if args.deap:
        processes.append(Process(target=run_dataset, args=('deap',)))
        
    for p in processes:
        p.start()
        
    for p in processes:
        p.join()