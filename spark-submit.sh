spark-submit --master yarn --deploy-mode cluster \
--py-files pysbdl_lib.zip \
--files conf/pysbdl.conf,conf/spark.conf,log4j.properties \
main.py qa 2022-08-02