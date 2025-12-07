import pytest

from spark.utils import build_spark_session


@pytest.fixture(scope="session")
def spark_session():
    spark = build_spark_session(app_name="citystream-tests")
    yield spark
    spark.stop()
