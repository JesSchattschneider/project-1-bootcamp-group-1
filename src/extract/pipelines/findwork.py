from dotenv import load_dotenv
import os
from src.extract.connectors.findwork_api import FindWorkApiClient
from src.extract.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from src.extract.assets.findwork import (
    extract_population,
    extract_jobs,
    transform_jobs,
    transform_population,
    load,
)

from src.extract.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from src.extract.assets.pipeline_logging import PipelineLogging
import yaml
from pathlib import Path
import schedule
import time


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
    search_query = "data engineer"

    # Extract jobs using the search query
    df_jobs = extract_jobs(
        findwork_api_client=findwork_api_client, search_query=search_query)

    # df_jobs = extract_jobs(
    #     findwork_api_client=findwork_api_client  # ,
    #     # city_reference_path=config.get("city_reference_path"),
    # )

    df_population = extract_population(
        population_reference_path=config.get("population_reference_path")
    )

    # transform
    pipeline_logging.logger.info("Transforming jobs dataframe")
    df_transformed_jobs = transform_jobs(df_jobs=df_jobs)
    print(df_transformed_jobs)

    # transform
    pipeline_logging.logger.info("Transforming population dataframe")
    df_transformed_population = transform_population(
        df_population=df_population)
    print(df_transformed_population)

    # load
    pipeline_logging.logger.info("Loading data to postgres")
    postgresql_client = PostgreSqlClient(
        server_name=os.environ.get("TARGET_SERVER_NAME"),
        database_name=os.environ.get("TARGET_DATABASE_NAME"),
        username=os.environ.get("TARGET_DB_USERNAME"),
        password=os.environ.get("TARGET_DB_PASSWORD"),
        port=os.environ.get("TARGET_PORT"),
    )
    metadata = MetaData()
    table_findwork = Table(
        "findwork_data",
        metadata,
        Column("job_id", String, primary_key=True),
        Column("job_title", String),
        Column("company_name", String),
        Column("company_num_employees", String),
        Column("employment_type", String),
        Column("job_location", String),
        Column("remote", String),
        Column("logo", String),
        Column("url", String),
        Column("job_description", String),
        Column("date_posted", String),
        Column("keywords", String),
        Column("source", String),
    )

    load(
        df=df_transformed_jobs,
        table=table_findwork,
        postgresql_client=postgresql_client,
        metadata=metadata,
    )

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
        df=df_transformed_population,
        table=table_population,
        postgresql_client=postgresql_client,
        metadata=metadata,
        load_method="overwrite"
    )

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

    # # set schedule
    # schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(run_pipeline, pipeline_name=PIPELINE_NAME, postgresql_logging_client=postgresql_logging_client, pipeline_config=pipeline_config)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
