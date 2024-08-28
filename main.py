import sys
from src.main import Utils
from src.main.logger import Log4j

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: {local, qa, prod} {load_date}: Arguments are missing")
        sys.exit(1)

    job_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    spark = Utils.get_spark_session(job_env)
    logger = Log4j(spark)

    print(spark)
    logger.info("Sucessfully tests the starter project!")


