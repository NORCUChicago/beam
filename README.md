# Beam

Beam is a deterministic record linkage tool for linking administrative data. The tool matches individuals across two datasets or deduplicates within one dataset using a set of personal identifying information (names, birthdate, ID, geography, etc.) and a set of rules to identify a set of unique individuals relevant to the research population.

This tool is applicable for 1-to-1, 1-to-many, many-to-many matches and deduplications. Each link is performed under three confidence levels (strict, moderate, relaxed), which reflect the level of certainty of match results, each codified by a specific set of rules. This allows for sensitivity testing of research results based on the strictness of match logic. The rules used by the deterministic matching algorithm are based on background knowledge and extensive testing on IL and Chicago administrative data.

Contact liu-aya@norc.org or sedovic-sabrina@norc.org for questions or support.

## Version

The latest version of Beam is **v1.4**. Past versions are not available on this repository.

See [Beam Version Tracker](docs/Beam%20Version%20Tracker.xlsx) for notes on current and past versions.

## Match Algorithm

Details about the current match algorithm are described in [Record Linkage v1.1 Match Logic](docs/Record%20Linkage%20v1.1%20Match%20Logic.docx).

### User-Defined Match Logic

If you would like to define your own blocking strategy and/or set of rules for accepting matches, please refer to `user_manual_user_defined_logic.md`.


## Setting Up

### Basic Requirements

Beam requires the following software on the secure server where matches are run:

- python
- tmux or other tools for running background jobs on server to prevent hang

Beam works with csv data as well as tables on Postgres databases. The Postgres database integration can be used for storing raw and preprocessed data, as well as using it as a scratch space for intemediary linkage steps. 

### Set up Git Repository

Clone this Git repository on an secure server for sensitive data like personal identfiable information.

Add the subdirectory `/shared` to your default `PYTHONPATH`. This directory contains code used by other modules in this repository. This step should be done differently based on your operating system:

#### Linux

Add the following line to `.bashrc` in your home directory:

```export PYTHONPATH='<root_directory>/beam_main/shared'```

where `<root_directory>` is the directory where this repository is stored. Restart the terminal to activate.

#### Windows

The setup on Windows differs by the software you use to run the code. Here are setups for Visual Studio Code and Spyder:

##### Visual Studio Code

Open `settings.json`, add:

```
"terminal.integrated.env.windows": {
        "PYTHONPATH":  '<root_directory>\\beam_main\\shared'
    }
```

where `<root_directory>` is the directory where this repository is stored. Restart the terminal to activate.

##### Spyder

Choose "PYTHONPATH manager" from the menu (python > PYTHONPATH manager) and add the path `<root_directory>\\beam_main\\shared`.
    

### Configure python Environment

To install the python packages needed for this tool, run:

```pip install -r requirements.txt```

in the directory of `requirements.txt`.

## Usage

### Define Data and Match Parameters

Edit ```config.py``` to define parameters of the input data, the match, and output. Follow the instructions in the comments of `config.py`.

### Preprocess Data

Beam's preprocessing step runs custom preprocessing scripts (based on a template) for input raw data files, combines relevant previously preprocessed data for a cumulative datasets for linkage, and saves out preprocessed data tables to Postgres (table name and schema are defined in `db_info` in `config.py`). **To use Beam's preprocessing functions, follow `user_manual_preprocessing.md`.**

For legacy fixed-width files that have already been preprocessed, run the following script to import preprocessed input data into Postgres for the match pipeline.

```python ./preprocessing/import_prepped_data.py```

### Run a Match From Beginning to End

Run a match from start to finish using already preprocessed data, including:
    - Matching (blocking, calculating similarity scores, accepting matched pairs)
    - Postprocessing
    - Calculating match rates

In a `tmux` session, run the following at the root of this directory.

```python run_match.py [-c CONFIG_JSON_FILEPATH] ```

A path to a configuration json file can be defined here with `-c` if using a copy saved in a different directory. By default, the script will use `./config.py` in the top level of this repository.

### Run a Match by Stage

Each of the following stage of the match can be run separately. See the scripts' docstring for how to run them.

- `matching/match.py`
- `postprocessing/postprocess.py`
- `match_rates/get_match_rates.py` (currently not applicable to M:M matches).

### Review differences between thresholds

To review pairwise matches that reflect the differences between strict, moderate, and relaxed thresholds, run the following line from the main directory:

    `python clerical_review/create_clerical_review_files.py`

This script produces 3 text files, for the following threshold groups:
    - strict and moderate
    - moderate and relaxed
    - relaxed and review

For each pass in the match file, the 100 matches with the lowest scores for
the higher threshold and the 100 matches with the highest scores for the lower
threshold are printed, along with a line indicating the cutoff point.

Note that the record linkage module does not use a score cutoff to determine
matches. The clerical review files are not to be used to determine what
score values to cutoff at, but instead ensure that the logic used is correctly
identifying matches.

## Repository Structure

### `./`
- `run_match.py`: central workflow script to run a match
- `config.py`: template for configuration file

### `clerical_review/`
Code for generating files used to review differences between thresholds.

### `docs/`
Expanded documentation for Beam.

### `match_rates/`
Code for analyzing match results.

### `matching/`
Code for blocking and generating pair-wise match results.

### `postprocessing/`
Code for postprocessing the match, generating ID crosswalks, and joining crosswalk to original data.

### `preprocessing/`
Code for preprocessing and importing data to Postgres.

### `shared/`
Code for shared functions used by scripts.