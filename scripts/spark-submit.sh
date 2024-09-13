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

https://iam.googleapis.com/projects/878514427384/locations/global/workloadIdentityPools/githubactionspool/providers/github-provider

gcloud projects add-iam-policy-binding adev-spark \
    --member='user:aravind.dev83@gmail.com' \
    --role='roles/iam.serviceAccountTokenCreator' \
    --impersonate-service-account ci-cd-service-account@adev-spark.iam.gserviceaccount.com

gcloud iam service-accounts add-iam-policy-binding wkf-oidc@adev-spark.iam.gserviceaccount.com \
    --member="principalSet://iam.googleapis.com/projects/878514427384/locations/global/workloadIdentityPools/githubactionspool/attribute.repository/Aravind-DEV83/Pyspark-project" \
   --role="roles/iam.serviceAccountTokenCreator"
