from tempfile import mkdtemp

import pytest

import shutil

@pytest.fixture
def temp_dir():
    temp_path = mkdtemp() 
    yield temp_path    
    shutil.rmtree(temp_path, ignore_errors=True)