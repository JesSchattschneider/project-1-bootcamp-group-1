from jinja2 import Environment, FileSystemLoader, Template
from graphlib import TopologicalSorter
from dotenv import load_dotenv
import os
from src.extract.connectors.findwork_api import FindWorkApiClient
from src.extract.connectors.postgresql import PostgreSqlClient
from sqlalchemy import create_engine, Table, inspect, MetaData, Column, Integer, String, Float, PrimaryKeyConstraint, DateTime
from src.extract.assets.findwork import (
    extract_population,
    extract_jobs,
    SqlTransform,
    transform,
    transform_jobs,
    transform_population,
    load,
    load2,
    extract,
    get_schema_metadata,
    transform2,
)

from sqlalchemy.engine import URL, Engine

from src.extract.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from src.extract.assets.pipeline_logging import PipelineLogging
import yaml
from pathlib import Path
import schedule
import time
import pandas as pd


def pipeline(config: dict, pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("Starting pipeline run")
    
    # set up environment variables
    pipeline_logging.logger.info("Getting pipeline environment variables")
    API_KEY = os.environ.get("API_KEY")

    pipeline_logging.logger.info("Creating Findwork API client")
    findwork_api_client = FindWorkApiClient(api_key=API_KEY)

    # extract
    pipeline_logging.logger.info(
        "Extracting data from Findwork API and CSV files")

    # Define the search query
    search_query = "data engineer" # todo: remove it as we are interested in all jobs
    
    # extract all jobs
    page = 1
    all_jobs = pd.DataFrame()
    location = None

    while True:
        df_jobs, has_more = extract_jobs(
            findwork_api_client, search_query, location, page)
        all_jobs = pd.concat([all_jobs, df_jobs], ignore_index=True)
        count = len(all_jobs)
        print(f"Total jobs: {count}")

        if not has_more:
            break
        page += 1

    df_jobs = all_jobs
    
    # load
    pipeline_logging.logger.info("Loading raw findwork data to postgres")
    postgresql_client = PostgreSqlClient(
        server_name=os.environ.get("TARGET_SERVER_NAME"),
        database_name=os.environ.get("TARGET_DATABASE_NAME"),
        username=os.environ.get("TARGET_DB_USERNAME"),
        password=os.environ.get("TARGET_DB_PASSWORD"),
        port=os.environ.get("TARGET_PORT"),
    )

    metadata = MetaData()
    table_findwork_raw = Table(
        "findwork_data_raw",
        metadata,
        Column("id", String, primary_key=True),
        Column("role", String),
        Column("company_name", String),
        Column("company_num_employees", String),
        Column("employment_type", String),
        Column("location", String),
        Column("remote", String),
        Column("logo", String),
        Column("url", String),
        Column("text", String),
        Column("date_posted", String),
        Column("keywords", String),
        Column("source", String),
    )

    load(
        df=df_jobs,
        table=table_findwork_raw,
        postgresql_client=postgresql_client,
        metadata=metadata,
    )
    
    # extract population data
    df_population = extract_population(
        population_reference_path=config.get("population_reference_path")
    )

    pipeline_logging.logger.info("Loading population data to postgres")

    metadata = MetaData()
    table_population = Table(
        "population_data",
        metadata,
        Column("population", Integer),
        Column("pop2024", Integer),
        Column("pop2023", Integer),
        Column("city", String),
        Column("country", String),
        Column("growthRate", Float),
        Column("type", String),
        Column("rank", Integer,primary_key=True),

    )

    load(
        df=df_population,
        table=table_population,
        postgresql_client=postgresql_client,
        metadata=metadata,
        load_method="overwrite"

    )

    # transform
    pipeline_logging.logger.info("Transforming jobs dataframe")
    df_transformed_jobs = transform_jobs(df_jobs=df_jobs.head(50)) # TODO: remove head and speed up transformation
    
    # load
    metadata = MetaData()
    table_findwork = Table(
        "findwork_data_clean",
        metadata,
        Column("job_id", String),
        Column("job_title", String),
        Column("company_name", String),
        Column("company_num_employees", String),
        Column("employment_type", String),
        Column("job_location", String),
        Column("remote", String),
        Column("logo", String),
        Column("url", String),
        Column("job_description", String),
        Column("date_posted", DateTime),
        Column("keywords", String),
        Column("source", String),
        Column("city", String),
        Column("country", String),
        PrimaryKeyConstraint('job_id', 'job_title', name='findwork_data_clean_pkey')
        # as we can have situations where the same job is posted multiple times
    )
    
    load(
        df=df_transformed_jobs,
        table=table_findwork,
        postgresql_client=postgresql_client,
        metadata=metadata,
        load_method="overwrite" # TODO: change to upsert!
    )

    pipeline_logging.logger.info("Transforming population dataframe")
    df_population_transformed = transform_population(df_population=df_population.head(100)) # TODO: remove head and speed up transformation
    
    # load
    metadata = MetaData()
    table_population = Table(
        "population_data_clean",
        metadata,
        Column("population", Integer),
        Column("pop2024", Integer),
        Column("pop2023", Integer),
        Column("city", String),
        Column("country", String),
        Column("growthrate", Float),
        Column("type", String),
        Column("rank", Integer),
    )

    load(
        df=df_population_transformed,
        table=table_population,
        postgresql_client=postgresql_client,
        metadata=metadata,
        load_method="overwrite" # TODO: change to upsert!
    )

    transform_template_environment = Environment(
            loader=FileSystemLoader(
                pipeline_config.get("config").get("transform_template_path")
            )
        )
    
    # create nodes
    staging_common_employment_type = SqlTransform(
            table_name="common_employment_type",
            postgresql_client=postgresql_client,
            environment=transform_template_environment,
        )
    
    dag = TopologicalSorter()
    dag.add(staging_common_employment_type)
    
    # run transform
    pipeline_logging.logger.info("Perform transform")
    transform(dag=dag)

    pipeline_logging.logger.info("Pipeline run successful")
    pipeline_logging.logger.info("Ending pipeline run")


def run_pipeline(
    pipeline_name: str,
    postgresql_logging_client: PostgreSqlClient,
    pipeline_config: dict,
):
    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_config.get("name"),
        log_folder_path=pipeline_config.get(
            "config").get("log_folder_path"),
    )
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config"),
    )
    try:
        metadata_logger.log()  # log start
        pipeline(
            config=pipeline_config.get("config"), pipeline_logging=pipeline_logging
        )
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )  # log end
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(
            f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )  # log error
        pipeline_logging.logger.handlers.clear()


if __name__ == "__main__":
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPELINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )

    run_pipeline(
        pipeline_name=PIPELINE_NAME,
        postgresql_logging_client=postgresql_logging_client,
        pipeline_config=pipeline_config,
    )


    ##emily - add the load parts to the destination database


    SOURCE_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    SOURCE_PORT = os.environ.get("TARGET_PORT")

    DEST_DATABASE_NAME = os.environ.get("DEST_DATABASE_NAME")
    DEST_SERVER_NAME = os.environ.get("DEST_SERVER_NAME")
    DEST_DB_USERNAME = os.environ.get("DEST_DB_USERNAME")
    DEST_DB_PASSWORD = os.environ.get("DEST_DB_PASSWORD")
    DEST_PORT = os.environ.get("DEST_PORT")

    source_connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=SOURCE_DB_USERNAME,
        password=SOURCE_DB_PASSWORD,
        host=SOURCE_SERVER_NAME,
        port=SOURCE_PORT,
        database=SOURCE_DATABASE_NAME,
    )
    source_engine = create_engine(source_connection_url)


    target_connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=DEST_DB_USERNAME,
        password=DEST_DB_PASSWORD,
        host=DEST_SERVER_NAME,
        port=DEST_PORT,
        database=DEST_DATABASE_NAME,
    )
    target_engine = create_engine(target_connection_url)



    extract_environment = Environment(loader=FileSystemLoader("./src/extract/pipelines/sql/extract"))

    for sql_path in extract_environment.list_templates():
        sql_template = extract_environment.get_template(sql_path)
        table_name = sql_template.make_module().config.get("source_table_name")
        data = extract(
            sql_template=sql_template,
            source_engine=source_engine,
            target_engine=target_engine,
        )
        source_metadata = get_schema_metadata(engine=source_engine)
        load2(
            data=data,
            table_name=table_name,
            engine=target_engine,
            source_metadata=source_metadata,
        )



    transform_environment = Environment(loader=FileSystemLoader("./src/extract/pipelines/sql/transform"))

    directory = "./src/extract/pipelines/sql/transform"
    for filename in os.listdir(directory):
        print(filename)
        transform_table_name = filename.replace(".sql","")
        transform_sql_template = transform_environment.get_template(
        filename
    )
        transform2(
            engine=target_engine,
            sql_template=transform_sql_template,
            table_name=transform_table_name,
        )




    # # set schedule
    # schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(run_pipeline, pipeline_name=PIPELINE_NAME, postgresql_logging_client=postgresql_logging_client, pipeline_config=pipeline_config)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
