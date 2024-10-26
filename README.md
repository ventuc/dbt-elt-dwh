# Datamart ELT Training with DBT

This project is the base for training on the development of ELT process for feeding a datamart with DBT and DuckDB. The included DuckDB database named `dwh` in file `dwh.ddb` is comprised of:
- sample source data in a staging schemata, preloaded using EL (Extract and Load) scripts:
    - schema `staging_scheduling_system`: sample data from a fictional operational database supporting a system for scheduling movies in a franchise of cinemas (Scheduling System)
    - schema `staging_booking_system`: sample data from a fictional operational database supporting a system for selling movie tickets in a franchise of cinemas (Booking System)
- a schema named `main_ods` representing the ODS (Operational Data Store) of a hyphotetical DWH built with DBT, already populated with data derived from the above mentioned data sourced.
- a schema named `main_datamart` representing the datamart of a hyphotetical DWH built with DBT: it's empty and will be populated by models developed in the DBT project.

## Data models

This repo provides also the models for each of the aforementioned schemata:
- `ods.png`: model of the ODS, which is the source of data for feeding the datamart, reflected in the `main_ods` schema
- `datamart.png`: model of the datamart, which is the target to be fed with data from the ODS, reflected in the `main_datamart` schema

## What's inside the DBT project

The provided DBT project includes:
- The definition of a profile defining a target which points to the DuckDB dwh database (see `profile.yml`)
- The defition of sources (see `sources.yml`)
- A seed CSV file representing data of movies provided by IMDB (see `seeds/imdb-movies.yml`)
- Models for feeding the tables of the ODS, which are provided only for reference, as they're already populated
- Two models for feeding the following dimension tables of the databart, which is initially empty (see `datamart_movie.sql` in `models/datamart`):
    -  `movie`
    -  `cinema`

Models for feeding the dimension table `screen` and the fact table `sale` of the datamart are provied in branch `datamart-models` of this repo.


## How to setup the project

1. After cloning the repo, create a Python virtual environment in it:

        python -m venv .dbtenv

2. Activate the newly created virtual environment:

    On Linux/MacOS:

        .dbtenv/bin/activate

    On Windows (using Powershell):

        Set-ExecutionPolicy Unrestricted -Scope Process
        .\.dbtenv\Scripts\activate


2. Install all required dependencies (including DBT):

        pip install -r requirements.txt


## How to run DBT

You can run the the provided DBT project straightaway, in order to see how DBT populates (incrementally) the `main_ods.movie` and `main_ods.projection` tables.

Afterwards you can develop other models for populating other tables.

The DBT project is named `dwh`, thus DBT has to been launched from inside the `dwh` folder. To run all the models that populate datamart tables run:

    cd dwh
    dbt build --select "datamart.*"

The `--select "datamart.*"` parameter tells DBT to run only models inside the `datamart` folder, which are the ones that populate the datamart. Therefore this parameter avoid launching models that populate ODS tables.

To run every model in the project simply launch DBT without the `--select` parameter.

