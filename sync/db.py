import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import pytz


class DatabaseManager:
    def __init__(self) -> None:
        self.db_a_config = {
            'host': os.getenv('DB_A_HOST'),
            'port': os.getenv('DB_A_PORT'),
            'database': os.getenv('DB_A_NAME'),
            'user': os.getenv('DB_A_USER'),
            'password': os.getenv('DB_A_PASSWORD'),
            'schema': os.getenv('DB_A_SCHEMA', 'public'),
        }
        self.db_b_config = {
            'host': os.getenv('DB_B_HOST'),
            'port': os.getenv('DB_B_PORT'),
            'database': os.getenv('DB_B_NAME'),
            'user': os.getenv('DB_B_USER'),
            'password': os.getenv('DB_B_PASSWORD'),
            'schema': os.getenv('DB_B_SCHEMA', 'public'),
        }

    def get_db_a_engine(self):
        conn = f"postgresql://{self.db_a_config['user']}:{self.db_a_config['password']}@{self.db_a_config['host']}:{self.db_a_config['port']}/{self.db_a_config['database']}"
        return create_engine(conn)

    def get_db_b_engine(self):
        conn = f"postgresql://{self.db_b_config['user']}:{self.db_b_config['password']}@{self.db_b_config['host']}:{self.db_b_config['port']}/{self.db_b_config['database']}"
        return create_engine(conn)

    def execute_query_to_dataframe(self, query: str, db_type: str = 'A') -> pd.DataFrame:
        engine = self.get_db_a_engine() if db_type.upper() == 'A' else self.get_db_b_engine()
        return pd.read_sql(query, engine)

    def upsert_dataframe_to_db(self, df: pd.DataFrame, table_name: str, unique_columns: list[str], db_type: str = 'B') -> None:
        engine = self.get_db_a_engine() if db_type.upper() == 'A' else self.get_db_b_engine()
        schema = self.db_a_config['schema'] if db_type.upper() == 'A' else self.db_b_config['schema']

        if 'last_synced' not in df.columns:
            df['last_synced'] = datetime.now(pytz.UTC)

        df = df.drop_duplicates(subset=unique_columns, keep='first')

        temp_table = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        df.to_sql(temp_table, engine, schema=schema, if_exists='replace', index=False)

        columns = df.columns.tolist()
        columns_str = ', '.join(columns)
        conflict_columns = ', '.join(unique_columns)
        update_columns = ', '.join([f"{c} = EXCLUDED.{c}" for c in columns if c not in unique_columns])

        upsert_sql = f"""
            INSERT INTO {schema}.{table_name} ({columns_str})
            SELECT {columns_str} FROM {schema}.{temp_table}
            ON CONFLICT ({conflict_columns}) DO UPDATE SET {update_columns}
        """

        with engine.connect() as conn:
            conn.execute(text(upsert_sql))
            conn.commit()

        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))
            conn.commit()


