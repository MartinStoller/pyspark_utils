# My PySpark Utils

Gathering generalizable PySpark helper functions.


## Environments
One challenge with a repository like this is the different Spark Environments.
Influenced by my personal history, I went for 2 separate directories:
- `databricks_utils` contains helper functions specific for databricks environments. When not working in Databricks or at least with components like Unity Catalog, MLFlow or Delta Lake, this part should be irrelevant for you.
- `generic_projects` tries to be less platform specific, however, it is influenced by my previous projects outside of Databricks, which is mostly AWS Glue or local setup. Even when working in Databricks, some files and functions in here might still be helpful.


## Project structure
The `generic_project` contains all kinds of files, functions and modules. Of course every project is
different and not every project needs to import all of them. 

The general idea is: 
- `common`: A module for the highest level of generalizability (across projects and domains).
- `shared`: Code that needs to be shared across otherwise strictly separated modules. So it is code, that is only generic **within** the specific project itself, but is not applicable across projects. In my experience this is mostly schemas, configs or enums. 
- `modules`: Strictly separated pipelines. They implement the generic ETL interface from the common module. I found that it is project specific how well this approach works. For environments that have naturally separated data and pipelines it works great, especially if pipelines are supposed to be triggered independently of each other (e.g. You dont always receive a full dataset). In environments with many dependencies between tables you risk running into a lot of circular dependencies this way, which leads to many intermediate steps and thus boilerplate code and architectural complexity. I usually try this approach first and as soon as I note too many dependencies, making a strict separation tough, I go for a different approach. Note, that a single module can implement multiple pipelines (e.g. `ingestion.py` for the bronze layer pipeline of this entity, and `post_processing.py` for remodeling the data according to your silver layer or so). I like to use the convention of each module only writing to a single table (thereby achieving separation of modules), and being named after the target table it is eventually supposed to populate. 
- `docs`: All Documentation (i like to use it mostly for ADRs and diagrams)
- `jobs`: The actual jobs, which get deployed. Typically, they just Orchestrate and call the individual ETLJobs from `modules`
- `docker`: If you develop locally, this might be useful to dockerize everything. (Deploying Spark, rarely requires Docker in my experience)
- `tests`: Typically, I create one test file per module.
- `test_data`: When developing locally, I store my mock data in here for development and - if necessary - for unittests.

## Logging
I just use print statements in this generic version. When a proper logging setup is needed, it needs
to be applied accordingly.

## Linting and Formatting
I like to use ruff, so I included a default setup.

## Schemas
Try to mostly avoid hardcoded strings as column names and instead use the schemas in `schemas.py`

## The ETLBase Interface
To keep the structure clean, i like to forbid the implementation of more methods than extract, transform, load, run.
All additional logic should go into `db.py` or `transformations.py` files in either the modue - or if generalizeable - in the shared or common modules.
This way we keep clean ETL classes and a clear structure, which really helps with readability and debugging.
I found that this structure is very intuitive to most people and new team members grasp and adopt it usually very quickly.


