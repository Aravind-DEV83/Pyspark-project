from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from logging import Logger

def createSparkSession():
    return SparkSession.builder. \
                        appName("GCS to BQ Pipeline"). \
                        getOrCreate()

if __name__ == "__main__":

    spark = createSparkSession()

    print(spark)
    

