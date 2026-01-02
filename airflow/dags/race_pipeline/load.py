from airflow.sdk import task
import pandas as pd
import sqlalchemy 
import logging
from sqlalchemy.engine import Engine
from datetime import timedelta
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DB_URL = "postgresql+psycopg2://postgres:postgres@db:5432/f1_db"

@task
def load(year : str, gp_name : str, **context) -> None:
    
    def get_data(context):

        session_data = context["ti"].xcom_pull(task_ids="transform", key = "processed_file")
    
        return pd.read_parquet(session_data, 
                               engine="pyarrow", 
                               dtype_backend="pyarrow"
                            )
    
    race_data : pd.DataFrame = get_data(context=context)
    race_data["Year"] = year
    race_data["Race"] = gp_name
    
    # logger.info(race_data.dtypes)
    
    # race_data["LapTime"].head(1).apply(
    #     lambda x: logger.info(type(x))
    # )
    
    # def to_timedelta(x):
    #     if isinstance(x, pd._libs.tslibs.timedeltas.Timedelta):
    #         return timedelta(seconds=x.total_seconds())
    #     elif x is None or pd.isna(x):
    #         return None
    #     else:
    #         raise ValueError(f"Unexpected type: {type(x)}")
    
    # race_data["LapTime"]= race_data["LapTime"].apply(to_timedelta)
    
    race_data.columns = [col.lower() for col in race_data.columns]
    logger.info(race_data.dtypes)
    
    logger.info(f"connecting to db with url \'{DB_URL}\'")
    
    engine : Engine = sqlalchemy.create_engine(url=DB_URL)
    
    with engine.connect() as conn:
        
        res = race_data.to_sql(name = "session_data", 
                        con = conn, 
                        if_exists = "append",
                        method = "multi",
                        index = False,
                        chunksize = 5000)
        
        logger.info(f"saved race {gp_name} to database, inserted {res} rows")
        
    
    