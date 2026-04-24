from emo_datasets import *
import warnings;
from config import *
import argparse
from multiprocessing import Process
warnings.filterwarnings('ignore')

datasets = {
    'biraffe': Biraffe,
    'case': Case,
    'deap': Deap
    }

def run_dataset(name, sample_size=None):
    dataset = datasets[name]()
    dataset.run(sample_size)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--biraffe', action='store_true')
    parser.add_argument('-c', '--case', action='store_true')
    parser.add_argument('-d', '--deap', action='store_true')
    parser.add_argument('-s', '--samples', type=int, default=None)
    args = parser.parse_args()

    if not any([args.biraffe, args.case, args.deap]):
        args.biraffe = True
        args.case = True
        args.deap = True

    processes = []

    if args.biraffe:
        processes.append(Process(target=run_dataset, args=('biraffe', args.samples)))
    if args.case:
        processes.append(Process(target=run_dataset, args=('case', args.samples)))
    if args.deap:
        processes.append(Process(target=run_dataset, args=('deap', args.samples)))
        
    for p in processes:
        p.start()
        
    for p in processes:
        p.join()