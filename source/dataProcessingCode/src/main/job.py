import logging
import Transformations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

# Variables
file_path = 'source/dataProcessingCode/src/main/data/uber_5.csv'
PROJECT_ID = 'adev-spark'
DATASET = 'temp'
TABLE_NAME = 'trip_data'
TEMPROARY_GCS_BUCKET = 'temp_gcs_adev-spark'
SERVICE_ACCOUNT_PATH = '/Users/aravind_jarpala/Downloads/Pyspark/Pyspark-project/source/dataProcessingCode/src/main/key.json'


def createSparkSession() -> SparkSession:

    spark = SparkSession.builder. \
                    appName("GCS to BQ Pipeline"). \
                    config("google.cloud.auth.service.account.enable", "true"). \
                    config("google.cloud.auth.service.account.json.keyfile", SERVICE_ACCOUNT_PATH). \
                    config("temporaryGcsBucket", TEMPROARY_GCS_BUCKET). \
                    config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"). \
                    config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"). \
                    config("spark.jars", "/Users/aravind_jarpala/Downloads/Pyspark/Pyspark-project/source/dataProcessingCode/src/main/lib/gcs-connector-hadoop2-latest.jar"). \
                    config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.40.0"). \
                    getOrCreate()
# ,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.10
    return spark
                        
                        
def dataLoad(spark: SparkSession) -> DataFrame:
    """Load Data from CSV file format

    :param spark - Spark Session Object
    :return DataFrame
    """
    df = spark.read. \
            format("csv"). \
            option("header", "true"). \
            option("inferSchema", "true"). \
            load(file_path)
    
    return df

def dataWrite(df: DataFrame) -> None:
    """"Write Data to BigQuery
    
    Keyword arguments:
    :param df - DataFrame
    :return None
    """

    df.write.format("bigquery"). \
            option("table",f'{PROJECT_ID}.{DATASET}.{TABLE_NAME}'). \
            mode("overwrite"). \
            save()

    return None
    
def main() -> None:
    """Pipeline Script Defination
    
    :return: None
    """

    spark = createSparkSession()
    logging.info("Spark Session Initiated sucessfully")

    
    input_df = dataLoad(spark)
    input_df.show(5)

    logging.info("Transformations -- IN ")
    Converted_df = Transformations.convertToTimeStamp(input_df)

    rideTime_df = Transformations.TotalRideTime(Converted_df)

    estimatedRideTime_df = Transformations.estimatedRideTime(rideTime_df)

    averageSpeed_df = Transformations.averageSpeed(estimatedRideTime_df)


    averageSpeed_df.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "total_ride_time", "total_estimated_ride_time", "average_speed"). \
                show()
    logging.info("Transformations -- OUT")

    dataWrite(averageSpeed_df)

    
    spark.stop()
    logging.info("Job finshed")
    
    return None


if __name__ == "__main__":
    main()
