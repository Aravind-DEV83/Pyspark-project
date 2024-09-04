import logging
import transformations
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

# Variables
INPUT_LOCATION='gs://input-source-adev-spark/uber_5.csv'
GCS_SPARK_CONNECTOR_LIB = 'gs://spark-lib-adev-spark/bigquery/gcs-connector-hadoop3-latest.jar'
PROJECT_ID = 'adev-spark'
DATASET = 'temp'
TABLE_NAME = 'trip_data'
TEMPROARY_GCS_BUCKET = 'temp_gcs_adev-spark'
SERVICE_ACCOUNT = '/Users/aravind_jarpala/Downloads/Pyspark/Pyspark-project/source/dataProcessingCode/src/main/key.json'


def create_spark_session() -> SparkSession:

    spark = SparkSession.builder. \
                    appName("GCS to BQ Pipeline"). \
                    config("google.cloud.auth.service.account.enable", "true"). \
                    config("google.cloud.auth.service.account.json.keyfile", SERVICE_ACCOUNT). \
                    config("temporaryGcsBucket", TEMPROARY_GCS_BUCKET). \
                    config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"). \
                    config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"). \
                    config("spark.jars", "/Users/aravind_jarpala/Downloads/Pyspark/Pyspark-project/source/dataProcessingCode/src/main/lib/gcs-connector-hadoop3-latest.jar"). \
                    config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.40.0"). \
                    getOrCreate()

    logging.info("Spark Session Initiated sucessfully")
    return spark

def data_load(spark: SparkSession) -> DataFrame:
    """Load Data from GCS

    :param spark - Spark Session Object
    :return DataFrame
    """
    input_df = spark.read. \
            format("csv"). \
            option("header", "true"). \
            option("inferSchema", "true"). \
            load(INPUT_LOCATION)

    return input_df

def data_write(output_df: DataFrame):
    """"Write Data to BigQuery

    Keyword arguments:
    :param df - DataFrame
    :return None
    """

    output_df.write.format("bigquery"). \
            option("table",f'{PROJECT_ID}.{DATASET}.{TABLE_NAME}'). \
            mode("overwrite"). \
            save()
    logging.info("Sucesfully upated data to BQ.")

def main():
    """Pipeline Script Defination

    :return: None
    """

    spark = create_spark_session()

    input_df = data_load(spark)
    input_df.show(5)

    logging.info("transformations -- IN ")
    converted_df = transformations.convert_to_timestamp(input_df)

    ride_time_df = transformations.total_ride_time(converted_df)

    estimated_ride_time_df = transformations.estimated_ride_time(ride_time_df)

    average_speed_df = transformations.average_speed(estimated_ride_time_df)

    average_speed_df. \
            select(
                "trip_distance", "total_ride_time", "total_estimated_ride_time", "average_speed"
            ). \
            show()
    logging.info("transformations -- OUT")

    data_write(average_speed_df)
    spark.stop()
    logging.info("Job finshed")


if __name__ == "__main__":
    main()
