# ODS ELT Training with DBT

This project is the base for training on the development of ELT process for feeding and ODS (Operational Data Store) with DBT and DuckDB. The included DuckDB database named `dwh` in file `dwh.ddb` is comprised of:
- sample source data in a staging schemata, preloaded using EL (Extract and Load) scripts:
    - schema `staging_scheduling_system`: sample data from a fictional operational database supporting a system for scheduling movies in a franchise of cinemas (Scheduling System)
    - schema `staging_booking_system`: sample data from a fictional operational database supporting a system for selling movie tickets in a franchise of cinemas (Booking System)
- a schema named `main_ods` representing the ODS (Operational Data Store) of a hyphotetical DWH built with DBT.

## Data models

This repo provides also the models for each of the aforementioned schemata:
- `scheduling_system.png`: model of the source database of the Scheduling System, reflected in the `staging_scheduling_system` schema
- `booking_system.png`: model of the source database of the Booking System, reflected in the `staging_booking_system` schema
- `imdb.png`: model of the data provided by IMDB in the `imdb_movie.csv` CSV file, which is included in the DBT prohject as seed
- `ods.png`: model of the ODS, which is the target to be fed with data from the source systems, reflected in the `main_ods` schema

## What's inside the DBT project

The provided DBT project includes:
- The definition of a profile defining a target which points to the DuckDB dwh database (see `profile.yml`)
- The defition of sources (see `sources.yml`)
- A seed CSV file representing data of movies provided by IMDB (see `seeds/imdb-movies.yml`)
- Models for feeding the tables `movie` and `projection` of the ODS, which is initially empty (see `.sql` files in `models/ods`).

Models for feeding the ticket table of the ODS are provied in branch `ods-model` of this repo.


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

The DBT project is named `dwh`, thus DBT has to been launched from inside the `dwh` folder:

    cd dwh
    dbt build

