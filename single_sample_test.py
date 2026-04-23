from dataset import *
from benchmark import benchmark_datasets
datasets = [Biraffe(), Case(), Deap()]

processed = {}
for dataset in datasets:
    processed[dataset.name] = dataset.run(sample_size=1)

results = benchmark_datasets(processed)
print(results)

