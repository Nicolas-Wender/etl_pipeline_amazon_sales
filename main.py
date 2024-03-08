from sqlalchemy import create_engine
from dotenv import dotenv_values
import os
import pandas as pd

env_vars = dotenv_values(".env")


def load_table_to_landing(df, engine, table_name):
    try:
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print("Table loaded successfully")
    except Exception as e:
        print(f"Error loading table: {e}")


def createEngine(credentials):
    try:
        engine = create_engine(credentials)
        print("Engine created successfully")
        return engine
    except Exception as e:
        print(f"Error creating engine: {e}")


def main():
    engine = createEngine(env_vars["CREDENTIALS_POSTGRESQL"])

    file_path = os.getenv("FILE_PATH")
    table_name = os.getenv("TABLE_NAME")

    """ Landing area """
    df = pd.read_excel(file_path)
    load_table_to_landing(df, engine, table_name)

    engine.dispose()


if __name__ == "__main__":
    main()
