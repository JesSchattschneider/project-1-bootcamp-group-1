from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader, Template

#### here is required modifications according to our datasets 


def extract(
    sql_template: Template,
    source_engine: Engine,
    target_engine: Engine,
) -> list[dict]:
    extract_type = sql_template.make_module().config.get("extract_type")
    if extract_type == "full":
        sql = sql_template.render()
        ##print(sql)
        return [dict(row) for row in source_engine.execute(sql).all()]
    elif extract_type == "incremental":
        # if target table exists :
        source_table_name = sql_template.make_module().config.get("source_table_name")
        if inspect(target_engine).has_table(source_table_name):
            incremental_column = sql_template.make_module().config.get(
                "incremental_column"
            )
            sql_result = [
                dict(row)
                for row in target_engine.execute(
                    f"select max({incremental_column}) as incremental_value from {source_table_name}"
                ).all()
            ]
            incremental_value = sql_result[0].get("incremental_value")
            sql = sql_template.render(
                is_incremental=True, incremental_value=incremental_value
            )
        else:
            sql = sql_template.render(is_incremental=False)
        print(sql)
        return [dict(row) for row in source_engine.execute(sql).all()]
    else:
        raise Exception(
            f"Extract type {extract_type} is not supported. Please use either 'full' or 'incremental' extract type."
        )


def get_schema_metadata(engine: Engine) -> Table:
    metadata = MetaData(bind=engine)
    metadata.reflect()  # get target table schemas into metadata object
    return metadata


def _create_table(table_name: str, metadata: MetaData, engine: Engine):
    existing_table = metadata.tables[table_name]
    new_metadata = MetaData()
    columns = [
        Column(column.name, column.type, primary_key=column.primary_key)
        for column in existing_table.columns
    ]
    new_table = Table(table_name, new_metadata, *columns)
    new_metadata.create_all(bind=engine)
    return new_metadata


def load(
    data: list[dict],
    table_name: str,
    engine: Engine,
    source_metadata: MetaData,
    chunksize: int = 1000,
):
    target_metadata = _create_table(
        table_name=table_name, metadata=source_metadata, engine=engine
    )
    table = target_metadata.tables[table_name]
    max_length = len(data)
    key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]
    for i in range(0, max_length, chunksize):
        if i + chunksize >= max_length:
            lower_bound = i
            upper_bound = max_length
        else:
            lower_bound = i
            upper_bound = i + chunksize
        insert_statement = postgresql.insert(table).values(
            data[lower_bound:upper_bound]
        )
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        engine.execute(upsert_statement)


def transform(engine: Engine, sql_template: Template, table_name: str):
    exec_sql = f"""
            drop table if exists {table_name};
            create table {table_name} as (
                {sql_template.render()}
            )
        """
    engine.execute(exec_sql)


if __name__ == "__main__":
    load_dotenv()

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



    extract_environment = Environment(loader=FileSystemLoader("sql/extract"))

    for sql_path in extract_environment.list_templates():
        sql_template = extract_environment.get_template(sql_path)
        table_name = sql_template.make_module().config.get("source_table_name")
        data = extract(
            sql_template=sql_template,
            source_engine=source_engine,
            target_engine=target_engine,
        )
        source_metadata = get_schema_metadata(engine=source_engine)
        load(
            data=data,
            table_name=table_name,
            engine=target_engine,
            source_metadata=source_metadata,
        )

#### until here is required modifications according to our datasets 

    transform_environment = Environment(loader=FileSystemLoader("sql/transform"))

    directory = "sql/transform"
    for filename in os.listdir(directory):
        transform_table_name = filename.replace(".sql","")
        transform_sql_template = transform_environment.get_template(
        filename
    )
        transform(
            engine=target_engine,
            sql_template=transform_sql_template,
            table_name=transform_table_name,
        )
