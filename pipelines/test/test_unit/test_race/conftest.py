from pipelines.race_pipeline.extract.extractor import RaceExtractor
from pipelines.race_pipeline.load.race_loader import RaceLoader

import tempfile
import pytest
import os
import shutil

@pytest.fixture
def temp_cache_dir():
    # create a temporary root
    tmp_root = tempfile.mkdtemp()
    # make the cache subdir inside
    cache_dir = os.path.join(tmp_root, "cache")
    os.makedirs(cache_dir, exist_ok=True)

    yield cache_dir  

    # cleanup 
    shutil.rmtree(tmp_root)

@pytest.fixture
def race_extractor():
    return RaceExtractor()

@pytest.fixture
def race_loader():
    return RaceLoader()
