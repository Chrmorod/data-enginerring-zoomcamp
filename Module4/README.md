# Module 4 dbt
---

## Commands

```bash
dbt --version                                                             
dbt init taxi_rides_ny
dbt debug
code ~/.dbt/profiles.yml
python3 ingest.py
duckdb -ui taxi_rides_ny.duckdb
dbt debug
```
## analyses
- A place for SQL files that you don't want to expose
- I generally use it for data quality reports
- Lots of people don't use it 
## dbt_project.yml
- The most important file in dbt
- Tell dbt some defaults
- You need it to run dbt commands
- For dbt core, your profile should match the one in the `.dbt/profile.yaml`
## macros
- They behave like Python functions (Reusable logic)
- They help you encapsulate logic (in one place)
## README.md
- The documentation of your project
- Installation/setup guides
- Contact information

## seeds
- A space to upload csv and flat files (to add them to dbt later)
- Quick and dirty approach (better to fix at source)

## snapshots
- Take a picture of a table at a moment in time
- Useful to track the history of a column that overwrites itself

## tests
- A place to put assertions in SQL format
- A place for singular tests 
- If this SQL command returns more than 0 rows, the dbt build fails

## models
- The most important directory
- dbt suggests 3 subfolders:
### staging
- Sources (so raw table from database)
- Staging files are 1 to 1 copy of your data with minimal cleaning steps:
  - Data types
  - Renaming columns
### intermediate
- Anything that is not raw nor you want to expose
- No guidelines, just nice for heavy duty cleaning or complex logic
### marts
- If it is in marts, it is ready for consumption
- Tables ready for dashboards
- Properly modeled, clean tables

