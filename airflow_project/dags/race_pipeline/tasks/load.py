from datetime import timedelta
import logging

from airflow.sdk import task
import pandas as pd

from hooks.my_postgres_hook import MyPostgresHook

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@task
def load(year : str, gp_name : str, conn_id : str = "POSTGRES_CONN", load_type : bool = "append", **context) -> None:
    
    def get_data(context):

        session_data = context["ti"].xcom_pull(task_ids="transform", key = "processed_file")
    
        return pd.read_parquet(session_data, 
                               engine="pyarrow", 
                               dtype_backend="pyarrow"
                            )
    
    race_data : pd.DataFrame = get_data(context=context)
    race_data["Year"] = year
    race_data["Race"] = gp_name
    race_data = race_data.reindex(sorted(race_data.columns), axis=1)


    race_data.columns = [col.lower() for col in race_data.columns]
    
    # Convert pyarrow duration to pandas timedelta, then to string for PostgreSQL INTERVAL
    race_data["laptime"] = race_data["laptime"].astype("timedelta64[ns]")

    # Convert timedelta to string format that PostgreSQL INTERVAL can parse
    # This preserves the duration value while ensuring proper type mapping
    race_data["laptime"] = race_data["laptime"].astype(str).where(
        race_data["laptime"].notna(), 
        None
    )
    
    logger.info(race_data.dtypes)
    
    with MyPostgresHook(conn_id=conn_id).get_conn() as conn:
            
        res = race_data.to_sql(name = "session_data", 
                        con = conn, 
                        if_exists = load_type,
                        method = "multi",
                        index = False,
                        chunksize = 5000)
    
    logger.info(f"saved race {gp_name} to database, inserted {res} rows")
        
    
    