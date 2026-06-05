import os
import sys

import pytest

# Make the pipeline package importable inside the container
sys.path.insert(0, "/opt/spark-apps")

from pipeline.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("test-pipeline")
    yield session
    session.stop()
