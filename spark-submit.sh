gcloud dataproc jobs submit pyspark gs://pyspark-files-adev-spark/gcs_to_bq/job.py \
    --cluster cluster-5904 \
    --region us-central1 \
    --py-files=gs://pyspark-files-adev-spark/gcs_to_bq/transformations.py,gs://pyspark-files-adev-spark/gcs_to_bq/constants.py \
    --jars=gs://spark-lib-adev-spark/gcs/gcs-connector-hadoop3-latest.jar \
    -- \
    --input_location gs://input-source-adev-spark/uber_5.csv \
    --project_id adev-spark \
    --temp_bucket temp_gcs_adev-spark \
    --output_table temp.trip_data

# python source/dataProcessingCode/src/main/job.py --input_location gs://input-source-adev-spark/uber_5.csv --project_id adev-spark --temp_bucket temp_gcs_adev-spark --output_table temp.trip_data

