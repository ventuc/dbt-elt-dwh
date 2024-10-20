# Sample DBT Project

This sample project is a base for experimentation with DBT using DuckDB. It includes a DuckDB database named `dwh` in file `dwh.ddb`, which includes:
- sample source data in a staging schemata, preloaded using EL (Extract and Load) scripts:
    - schema `staging_scheduling_system`: sample data from a fictional operational database supporting a system for scheduling movies in a franchise of cinemas
    - schema `staging_booking_system`: sample data from a fictional operational database supporting a system for selling movie tickets in a franchise of cinemas
- a schema named `main_ods` representing the ODS (Operational Data Store) of a hyphotetical DWH built with DBT.

## What's inside the DBT project

The provided DBT project includes:
- The definition of a profile defining a target which points to the DuckDB dwh database (see `profile.yml`)
- The defition of sources (see `sources.yml`)
- A seed CSV file representing data of movies provided by IMDB (see `seeds/imdb-movies.yml`)
- Models for feeding the movie table of the ODS, which is initially empty (see `.sql` files in `models/ods`).


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

You can run the the provided DBT project straightaway, in order to see how DBT populates (incrementally) the `main_ods.movie` table.

Afterwards you can develop other models for populating other tables.

The DBT project is named `dwh`, thus DBT has to been launched from inside the `dwh` folder:

    cd dwh
    dbt build

