Coffee Market Analysis
==============================

Data manipulation scripts to support my <a target="_blank" href="https://public.tableau.com/views/GlobalCoffeeProduction/Coffee?:language=en-US&:display_count=n&:origin=viz_share_link">Tableau vizualization</a> of coffee market data from the <a target="_blank" href="https://ico.org">International Coffee Organization.</a>

The `/src/data` folder within this repo contains two python scripts: 
1. `etl_functions.py` contains functions that reshape and manipulate excel data. 
2. `make_dataset.py` utilizes the functions in `etl_functions.py` to transform the raw excel spreadsheet data into a tidy format for use in Tableau.

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data reference document from the ICO, links to data sources.
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   └── data           <- Scripts to download or generate data
    │       ├── etl_functions.py
    |       └── make_dataset.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
