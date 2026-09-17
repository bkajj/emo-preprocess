# Preprocessing effects in physiological emotion recognition

My master's thesis on the impact of sensor data preprocessing methods on emotion prediction, compared across three datasets.

## Motivation
Most affective computing papers treat preprocessing as an implementation detail and often describe it only briefly. Models are usually trained on a single dataset, which makes results difficult to compare across datasets. This work reverses that perspective and treats preprocessing as the main object of study, using a fixed model as the measuring tool.

## Overview
The project unifies three datasets into a single pipeline comparing four main preprocessing decisions: 
- feature selection
- window size
- missing value handling
- per-subject normalization
The results are measured in two validation modes: subject-independent (SI) and subject-dependent (SD). A control experiment repeats the reference configuration with two additional models to verify that the results do not depend on the choice of model.

## Datasets

The datasets are not included in this repository and must be downloaded separately:

- **BIRAFFE2** - https://zenodo.org/records/5786104
- **CASE** - https://gitlab.com/karan-shr/case_dataset/
- **DEAP** - https://www.kaggle.com/datasets/manh123df/deap-dataset

DEAP dataset is distributed by Queen Mary University of London under an EULA that must be signed by a permanent member of academic staff; the official request page (http://www.eecs.qmul.ac.uk/mmv/datasets/deap/download.html) was unavailable at the time of writing.

Place each dataset in the parent directory, as shown in the project structure below.

## Project structure

The code expects the datasets in the parent directory:

```
.
├── BIRAFFE2/
├── CASE/
├── DEAP/
├── results/        # experiment output
└── src/            # this repository
```

Inside `src`:

```
src/
├── pipeline.py            # entry point — runs a single experiment
├── config.py              # configuration loading and paths
├── preprocess.py          # segmentation and feature extraction
├── common.py              # imputation, normalization, CV splitters
├── regression.py          # model training and evaluation
├── configs/               # experiment configurations (YAML)
├── emo_datasets/          # dataset-specific loading
└── requirements.txt
```

Additional scripts were used for data inspection and generating figures.

## Setup

Requires Python 3.13

```powershell
python -m venv .venv
.venv\Scripts\activate        # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Each experiment is defined by a YAML configuration file in `configs/`.
A run is started by passing the configuration path to the pipeline:

```powershell
python pipeline.py --config configs/baseline.yaml
```

`baseline.yaml` is the reference configuration used throughout the thesis and
documents all available options. The remaining files change one element at a time:

| Configuration | Varied element |
| --- | --- |
| `baseline` | reference configuration |
| `baseline_v2` | alternative window length per dataset |
| `5s` | uniform 5-second window for all datasets |
| `baseline_impute_mean` | mean imputation instead of median |
| `baseline_drop_nans` | rows with missing values removed |
| `baseline_no_normalization` | per-subject normalization disabled |
| `gradient_boosting` | control experiment with a gradient boosting model |
| `ridge_regression` | control experiment with a ridge regression model |
| `noise/noise1`–`noise10` | reference configuration repeated with random seeds |

## Output

Each run writes to a timestamped directory under `results/`, containing the
metrics for both validation modes, per-dataset feature tables, and a copy of the configuration used.
Extracted features and preprocessed signals are cached between runs, so
repeated experiments reuse them instead of recomputing.

## Citation

O. Zwolak, *Comparative analysis of sensor data preprocessing methods and their impact on emotion prediction*, master's thesis,
Jagiellonian University, Kraków, 2026.