import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

def main(result_path):
    datasets = ['deap', 'case', 'biraffe']
    features = ['HRV_SDNN', 'HRV_RMSSD']
    ranges = {'HRV_SDNN': (32, 93), 'HRV_RMSSD': (19, 75)}

    dfs = {}
    for d in datasets:
        df = pd.read_csv(os.path.join(result_path, d.upper() + '.csv'))
        dfs[d] = df

    fig, axes = plt.subplots(1, 2, figsize=(10, 5), sharey=True)

    for ax, feat in zip(axes, features):
        data = [dfs[d][feat].dropna() for d in datasets]
        ax.boxplot(data, labels=['DEAP', 'CASE', 'BIRAFFE2'], flierprops=dict(marker='o', markersize=5, alpha=0.3))
        ax.set_yscale('log')
        ax.axhspan(*ranges[feat], alpha=0.08, color='green', label='zakres wartości raportowanych')
        ax.axhline(ranges[feat][0], color='green', linewidth=0.5, alpha=0.15)
        ax.axhline(ranges[feat][1], color='green', linewidth=0.5, alpha=0.15)
        ax.grid(axis='y', which='major', alpha=0.2)
        ax.set_title(feat[4:])
        
        if ax is axes[1]:
            ax.legend()
        else:
            ax.set_ylabel('ms')


    fig.tight_layout()
    fig.savefig('hrv_graph.pdf')   
    plt.show()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Specify result folder path in args")
        sys.exit(1)

    main(sys.argv[1])