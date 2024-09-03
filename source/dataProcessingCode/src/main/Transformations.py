from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, when, round
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

def convertToTimeStamp(df: DataFrame) -> DataFrame:
    """Convert String to Timestamp
    
    Keyword arguments:
    :param df - Input DataFrame
    :return DataFrame
    """

    convertedDf = df. \
        withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))). \
        withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))

    return convertedDf


def TotalRideTime(df: DataFrame) -> DataFrame:
    """Calucate the total time taken for the ride.
    
    Keyword arguments:
    :param df - Input DataFrame
    :return DataFrame
    """

    rideTime_df = df. \
                    withColumn("total_ride_time", when(
                        col("tpep_pickup_datetime") != col("tpep_dropoff_datetime"), 
                        round(col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long"))
                    ).otherwise(0)
                    )
    return rideTime_df

def estimatedRideTime(df: DataFrame) -> DataFrame:
    """Calucate the estimated time taken for the ride.
    
    Keyword arguments:
    :param df - Input DataFrame
    :return DataFrame
    """
    average_speed = 30

    estimated_df = df. \
                    withColumn("total_estimated_ride_time", 
                        round(col("trip_distance") / average_speed * 60 * 60)
                    )
    return estimated_df

def averageSpeed(df: DataFrame) -> DataFrame:
    """Calucate the average speed taken for the ride.
    
    Keyword arguments:
    :param df - Input DataFrame
    :return DataFrame
    """
    speed_df = df. \
                withColumn("average_speed", when(
                    col("total_ride_time")!=0, 
                    round( (col("trip_distance") / col("total_ride_time")) * 3600)
                ).otherwise(0)
                )
    return speed_df
