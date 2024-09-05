spark-submit --master yarn --deploy-mode cluster \
--py-files pysbdl_lib.zip \
--files conf/pysbdl.conf,conf/spark.conf,log4j.properties \
main.py qa 2022-08-02

# python source/dataProcessingCode/src/main/job.py --input_location gs://input-source-adev-spark/uber_5.csv --project_id adev-spark --temp_bucket temp_gcs_adev-spark --output_table temp.trip_data