## credits to Bas_P_Harenslak,_Julian_Rutger_de_Ruiter_Data_Pipelines_with_Apache

import glob
import importlib.util
import os

import pytest
from airflow.models import DAG

DAG_PATH = os.path.join(
    os.path.dirname(__file__), "..","..", "dags/**/*_dag.py"
)

# list of found dags ending with _dag.py
DAG_FILES = glob.glob(DAG_PATH, recursive=True)

# parametrize runs test_dag_integrity(dag_file) for every dag (dag_file) in DAG_FILES  
@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file):
    # get extension (.py)
    module_name, _ = os.path.splitext(dag_file)
    
    # get path to the module
    module_path = os.path.join(DAG_PATH, dag_file)
    
    # get module spec
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    
    # create a module from provided spec
    module = importlib.util.module_from_spec(mod_spec)
    
    # execute module
    mod_spec.loader.exec_module(module)
    
    # vars(module) provides a dict of module objects in its namespace
    # get dag objects found in module objects
    dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
    
    assert dag_objects
    
    for dag in dag_objects:
        dag.check_cycle()
    