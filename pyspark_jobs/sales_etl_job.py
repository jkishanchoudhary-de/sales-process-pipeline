import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--process_date")

args = parser.parse_args()

date = args.process_date

spark = SparkSession.builder.appName("sales-count").getOrCreate()

# Input file path
input_path = f"gs://airflow-pysaprk-dags/input/sales_{date}.csv"

df = spark.read.option("header", True).csv(input_path)

count = df.count()

result_df = spark.createDataFrame(
    [(date, count)],
    ["process_date", "record_count"]
)

# Write result to GCS
output_path = f"gs://airflow-pysaprk-dags/output/sales_count_{date}"

result_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

spark.stop()