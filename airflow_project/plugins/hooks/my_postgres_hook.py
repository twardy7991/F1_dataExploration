from typing import Generator
from contextlib import contextmanager
import logging

from airflow.sdk.bases.hook import BaseHook
from airflow.models import Connection
import sqlalchemy 
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class MyPostgresHook(BaseHook):
    
    def __init__(self, conn_id) -> None:
        super().__init__()
        self.conn_id = conn_id
        self._engine = self._get_engine()
        
    def _get_engine(self) -> Engine:
        conn: Connection = self.get_connection(self.conn_id)
        return sqlalchemy.create_engine(
            conn.get_uri()
        )

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = self._get_engine()
        return self._engine
    
    # left in case we want to switch to "manual" queries instead of pandas's to_sql 
    # @property
    # def session_factory(self) -> sessionmaker:
    #     if self._session_factory is None:
    #         self._session_factory = sessionmaker(
    #             bind=self.engine,
    #             autocommit=False,
    #             autoflush=False,
    #         )
    #     return self._session_factory
        
    @contextmanager
    def get_conn(self) -> Generator[Connection, None, None]:
        conn = self.engine.connect()
    
        try:
            yield conn
        finally:
            conn.close()
        