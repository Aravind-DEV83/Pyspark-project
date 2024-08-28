import pytest
from src.main.Utils import get_spark_session

@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")

def blank_test(spark):
    print(spark.version)
    assert spark.version == "3.5.0"



